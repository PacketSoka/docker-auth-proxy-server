import os
import socket
import sys
from datetime import datetime, timedelta
from email.parser import BytesParser
import select

# address for proxy server
HOST_SERVER = '0.0.0.0'
PORT_SERVER = int(os.getenv('LISTEN_PORT'))

# address for clusterIp service
HOST_FORWARD = os.getenv('REGISTRY_ADRESS')
PORT_FORWARD = int(os.getenv('PORT_REGISTRY_SERVICE'))

# address for docker private registry
HOST_REGISTRY = os.getenv('HOST_REGISTRY').encode('utf-8')
PORT_REGISTRY = os.getenv('PORT_REGISTRY').encode('utf-8')

BUFFER_SIZE = 4096
BAD_RESPONSE = (b'HTTP/1.1 401 Unauthorized\r\n'
                b'Content-Type: application/json; charset=utf-8\r\n'
                b'Docker-Distribution-Api-Version: registry/2.0\r\n'
                b'Www-Authenticate: Basic realm="Registry Realm"\r\n'
                b'X-Content-Type-Options:nosniff\r\n'
                b'Content-Length: 48\r\n'
                b'\r\n'
                b'{"errors": "permission denied for PULL and GET"}\r\n'
                b'\r\n')

FAKE_CRED = os.getenv('CRED_USERS').encode('utf-8')
ORIG_CRED = os.getenv('CRED_ROOT').encode('utf-8')


class ParserHttp:
    def __init__(self, data):
        self.raw_data = data
        self.method = None
        self.url = None
        self.protocol = None
        self.headers = None
        self.parse()

    def parse(self):
        start_line, headers = self.raw_data.split(b'\r\n', 1)
        self.method, self.url, self.protocol = start_line.split(b' ', 2)
        self.headers = BytesParser().parsebytes(headers)

    def modify_auth(self, new_auth, orig_auth):
        return self.raw_data.replace(orig_auth, new_auth)


class Forward:
    def __init__(self):
        self.forward = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self, host, port):
        try:
            self.forward.connect((host, port))
            return self.forward
        except Exception as e:
            print(f"[exception] - {e}")
            return False


class ProxyServer:
    input_list = []
    channel = {}

    def __init__(self, host, port):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((host, port))
        self.server.listen(200)
        self.registry_cache = {}

    def main_loop(self):
        self.input_list.append(self.server)
        while True:
            inputready, outputready, exceptready = select.select(self.input_list, [], [])
            for s in inputready:
                if s == self.server:
                    self.on_accept(s)
                    break
                try:
                    self.data = s.recv(BUFFER_SIZE)
                except ConnectionResetError:
                    print(f"ConnectionResetError: {s}")
                    self.on_close(s)
                    break
                if len(self.data) == 0:
                    self.on_close(s)
                    break
                else:
                    self.on_recv(s)

    def on_accept(self, s):
        forward = Forward().start(HOST_FORWARD, PORT_FORWARD)
        clientsock, clientaddr = s.accept()
        if forward:
            self.input_list.append(clientsock)
            self.input_list.append(forward)
            self.channel[clientsock] = forward
            self.channel[forward] = clientsock
        else:
            print(f"Can't establish a connection with remote server. Closing connection with client side {clientaddr}")
            clientsock.close()

    def on_close(self, s):
        # print(f"{s.getpeername()} has disconnected")
        #remove objects from input_list
        self.input_list.remove(s)
        self.input_list.remove(self.channel[s])
        out = self.channel[s]
        # close the connection with client
        self.channel[out].close()
        # close the connection with remote server
        self.channel[s].close()
        # delete both objects from channel dict
        del self.channel[out]
        del self.channel[s]

    def on_recv(self, s):
        data = self.data
        if self.parse_client_requests(s, data):
            return
        self.channel[s].send(data)

    # parse data in socket
    def parse_client_requests(self, s, data):
        if s.getsockname()[1] == PORT_SERVER:
            try:
                parse_http = ParserHttp(data)
            except Exception:
                return False
            mod_data = self.check_cred(parse_http)
            if mod_data:
                if not self.check_method(mod_data, s):
                    s.send(BAD_RESPONSE)
                    return True
                self.channel[s].send(mod_data)
                return True
        return False

    def is_repo_in_registry(self, s, repo_name):
        if repo_name in self.registry_cache:
            if datetime.now() - self.registry_cache[repo_name]['last_updated'] < timedelta(seconds=10):
                return self.registry_cache[repo_name]['val']
            del self.registry_cache[repo_name]
        req_msg = (b'HEAD /v2/' + repo_name + b'/manifests/latest HTTP/1.1\r\n'
                   b'Host: ' + HOST_REGISTRY + b':' + PORT_REGISTRY + b'\r\n'
                   b'Authorization: ' + ORIG_CRED + b'\r\n'
                   b'User-Agent: curl/8.7.1\r\n'
                   b'Accept:'
                                                    b' application/vnd.oci.image.manifest.v1+json,'
                                                    b' application/vnd.oci.image.index.v1+json,'
                                                    b' application/vnd.docker.distribution.manifest.v2+json,'
                                                    b' application/vnd.docker.distribution.manifest.list.v2+json,'
                                                    b' application/vnd.docker.distribution.manifest.v1+json\r\n'
                   b'\r\n')
        self.channel[s].send(req_msg)
        resp = b''
        while True:
            r_ready, _, _ = select.select([self.channel[s]], [], [], 0.1)
            if len(r_ready) == 0:
                break
            data = self.channel[s].recv(BUFFER_SIZE)
            if data:
                resp += data
        if resp.split(b' ')[1] == b'404':
            self.registry_cache[repo_name] = {'last_updated': datetime.now(), 'val': False}
            return self.registry_cache[repo_name]['val']
        self.registry_cache[repo_name] = {'last_updated': datetime.now(), 'val': True}
        return self.registry_cache[repo_name]['val']

    def check_method(self, data, s):
        parse_http = ParserHttp(data)
        repo_name = parse_http.url.split(b'/')[2]
        match parse_http.method:
            case b'GET':
                if parse_http.url != b'/v2/':
                    return False
            case b'POST' | b'PUT' | b'HEAD':
                if self.is_repo_in_registry(s, repo_name):
                    return False
        return True

    @staticmethod
    def check_cred(req):
        if "Authorization" in req.headers.keys():
            if req.headers["Authorization"] == FAKE_CRED.decode("utf-8"):
                mod_data = req.modify_auth(ORIG_CRED, req.headers["Authorization"].encode("utf-8"))
                return mod_data
        return None


if __name__ == '__main__':
    server = ProxyServer(HOST_SERVER, PORT_SERVER)
    try:
        server.main_loop()
    except KeyboardInterrupt:
        print("Ctrl C - Stopping server")
        sys.exit(1)

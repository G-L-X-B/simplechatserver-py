#!/usr/bin/env python3
from argparse import ArgumentParser
from json import dumps, loads
from socket import socket
from socket import SHUT_WR


HOST = ''
PORT = 2078
TOKEN = '2drrtq227vwuu82vzz5b'


def main():
    request = create_request()
    s = socket()
    s.connect((HOST, PORT))
    s.sendall(dumps(request).encode())
    s.shutdown(SHUT_WR)
    response = loads(read_all(s))
    match response.get('status' ):
        case None:
            print('Server status:')
            print(f'Messages stored: {response["stored"]}')
            print(f'Total messages recieved: {response["total"]}')
            print(f'Storage limit: {response["size limit"]}')
        case 'ok':
            print(f'Server will shutdown down in {request["timeout"] // 1000} seconds.')
        case 'err':
            print(f'Server error: {response["what"]}')


def create_request() -> dict:
    parser = ArgumentParser(prog='admin')
    parser.add_argument('command', choices=('status', 'shutdown'),
                        default='status')
    parser.add_argument('-t', '--timeout', default=10, type=int)
    args = parser.parse_args()

    request = {
        'action': args.command,
        'token': TOKEN
    }
    if args.command == 'shutdown':
        request['timeout'] = args.timeout * 1000
    
    return request


def read_all(s):
    res = bytearray()
    buf = s.recv(1024)
    while len(buf) != 0:
        res += buf
        buf = s.recv(1024)
    return res.decode()


if __name__ == '__main__':
    main()

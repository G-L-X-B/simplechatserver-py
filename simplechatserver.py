from asyncio import StreamReader, StreamWriter, TaskGroup
from asyncio import run, start_server
from asyncio import sleep as asleep
from collections import deque, namedtuple
from contextvars import Context
from json import JSONDecodeError
from json import dumps, loads


HOST = ''
CLIENT_PORT = 2077
ADMIN_PORT = 2078


Message = namedtuple('Message', ('who', 'n', 'text'))

# These are client requests and responses
ex_ud_request = {
    'action': 'get',
    'last': 1448
}

ex_ud_response = {
    'status': 'ok',
    'timeout': 1000,
    'messages': [
        {'who': 'nickname', 'n': 1449, 'text': 'some text'},
        {'who': 'nickname', 'n': 1450, 'text': 'some text'},
    ]
}

ex_err_response = {
    'status': 'err',
    'what': 'JSON parse error'  # whatever
}

ex_ms_request = {
    'action': 'post',
    'nick': 'glxb',
    'text': 'some text'
}

ex_ms_response = {
    'status': 'ok',
    'timeout': 1000
}

# These are admin requests and responses

ex_status_request = {
    'action': 'status',
    'token': '1234567890abcdefghij'
}

ex_status_response = {
    'total': 12789,
    'stored': 945,
    'size limit': 1000
}


class ShutdownException(Exception):
    pass


class SimpleChatServer:
    def __init__(self, storage_limit: int = 1000):
        self.total = 0
        self.messages = deque(maxlen=storage_limit)
        self.admin_token = '2drrtq227vwuu82vzz5b'
    
    async def run(self):
        server = await start_server(
            self.handle_connection,
            HOST,
            CLIENT_PORT
        )
        admin_server = await start_server(
            self.handle_admin,
            HOST,
            ADMIN_PORT
        )

        self.task_group = TaskGroup()
        try:
            async with self.task_group as tg:
                self.server_task = tg.create_task(server.serve_forever())
                self.admin_task = tg.create_task(admin_server.serve_forever())
        except* ShutdownException:
            print('Shutdown was requested and performed.')
        
    async def handle_connection(self, reader: StreamReader, writer: StreamWriter):
        peer = writer.get_extra_info('peername')
        print(f'{peer} connected')
        read_task = self.task_group.create_task(reader.read(-1))
        await read_task
        data = read_task.result()

        print(f'{peer}: {data.decode()}')

        response = self.process_input(data, self.handle_request)

        print(f"Responding to {peer} with '{response.decode()}'")

        writer.write(response)
        writer_task = self.task_group.create_task(writer.drain())
        await writer_task

        writer.close()
        writer_task = self.task_group.create_task(writer.wait_closed())
        await writer_task

    def process_input(self, data: bytes, handler) -> dict:
        response = {}
        try:
            request = self.parse_request(data)
            response = handler(request)
        except UnicodeError:
            response = form_error_response(
                'Unicode error: check message encoding to be utf-8')
        except JSONDecodeError as e:
            response = form_error_response(f'JSON error: {e}')
        except KeyError as e:
            response = form_no_field_in_request_error_response(e.args[0])
        return dumps(response).encode()

    def parse_request(self, data: bytes) -> dict:
        return loads(data.decode())

    def handle_request(self, request: dict) -> dict:
        response = {}
        match request['action']:
            case 'get':
                response = self.retrieve_messages(request)
            case 'post':
                response = self.post_message(request)
            case _:
                response = self.form_request_format_error_response(
                    f'invalid action is passed ({_})')
        return response

    def retrieve_messages(self, request: dict) -> dict:
        response = {}
        try:
            n = request['last']
            if n > self.total:
                raise ValueError(n)
            else:
                response = {
                    'status': 'ok',
                    'timeout': 1000,
                    'messages': sorted(
                        map(
                            lambda m: m._asdict(),
                            filter(
                                lambda m: m.n > n,
                                self.messages
                            )
                        ),
                        key=lambda m: m['n']
                    )
                    #'messages': [{'who': 'nickname', 'n': 1, 'text': 'some text'}]
                }
        except KeyError:
            response = form_no_field_in_request_error_response('last')
        except ValueError as e:
            response = form_error_response(
                f'requested message number is too big ({e.args[0]}, last is {data["last"]})')
        return response

    def post_message(self, request: dict) -> dict:
        response = {}
        try:
            nick = request['nick']
            text = request['text']
            self.total += 1
            self.messages.append(Message(nick, self.total, text))
            response = {
                'status': 'ok',
                'timeout': 1000
            }
        except KeyError as e:
            response = form_no_field_in_request_error_response(e.args[0])
        return response


    async def handle_admin(self, reader: StreamReader, writer: StreamWriter):
        peer = writer.get_extra_info('peername')
        print(f'Admin have connected from {peer}')
        reader_task = self.task_group.create_task(reader.read(-1))
        await reader_task
        data = reader_task.result()
        
        print(f'Admin {peer}: {data.decode()}')

        response = self.process_input(data, self.handle_admin_request)

        print(f"Responding to amdin {peer} with '{response.decode()}'")

        writer.write(response)
        writer_task = self.task_group.create_task(writer.drain())
        await writer_task

        writer.close()
        await writer.wait_closed()
    
    def handle_admin_request(self, request: dict) -> dict:
        response = {}
        if request['token'] != self.admin_token:
            response = form_error_response('Invalid password.')
        else:
            match request['action']:
                case 'status':
                    response = self.form_status_response()
                case 'shutdown':
                    response = self.schedule_shutdown(request)
                case _:
                    response = self.form_request_format_error_response(
                        f'invalid action is passed ({_})')
        return response

    def form_status_response(self):
        return {
            'total': self.total,
            'stored': len(self.messages),
            'size limit': self.messages.maxlen
        }
    
    def schedule_shutdown(self, request: dict):
        timeout = int(request['timeout'])
        self.shut_task = self.task_group.create_task(self.shutdown(timeout // 1000))
        return {
            'status': 'ok',
        }

    async def shutdown(self, wait_time):
        await asleep(wait_time)
        raise ShutdownException()


def form_error_response(about: str) -> dict:
    return {
        'status': 'err',
        'what': about
    }


def form_request_format_error_response(about: str) -> dict:
    return form_error_response(f'Request format error: {about}')


def form_no_field_in_request_error_response(field: str) -> dict:
    return form_request_format_error_response(f'no "{field}" field specified')


if __name__ == '__main__':
    server = SimpleChatServer()
    run(server.run())

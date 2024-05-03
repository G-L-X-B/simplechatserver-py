from asyncio import run, start_server, StreamReader, StreamWriter
from collections import namedtuple
from json import dumps, JSONDecodeError, loads


HOST = ''
PORT = 2077


Message = namedtuple('Message', ('who', 'n', 'text'))


data = {
    'last': 0,
    'messages': []
}


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


def form_error_response(about: str) -> dict:
    return {
        'status': 'err',
        'what': about
    }


def form_request_format_error_response(about: str) -> dict:
    return form_error_response(f'Request format error: {about}')


def form_no_field_in_request_error_response(field: str) -> dict:
    return form_request_format_error_response(f'no "{field}" field specified')


async def handle_connection(reader: StreamReader, writer: StreamWriter):
    peer = writer.get_extra_info('peername')
    print(f'{peer} connected')
    data = await reader.read(-1)

    print(f'{peer}: {data.decode()}')

    response = handle_request(data)

    a = dumps(response)
    print(f"Responding to {peer} with '{a}'")

    writer.write(dumps(response).encode())
    await writer.drain()

    writer.close()
    await writer.wait_closed()


def handle_request(data: bytes) -> dict:
    response = {}
    try:
        request = parse_request(data)
        response = form_response(request)
    except UnicodeError:
        response = form_error_response(
            'Unicode error: check message encoding to be utf-8')
    except JSONDecodeError as e:
        response = form_error_response(f'JSON error: {e}')
    except KeyError:
        response = form_no_field_in_request_error_response('action')
    return response


def parse_request(data: bytes) -> dict:
    return loads(data.decode())


def form_response(request: dict) -> dict:
    response = {}
    match request['action']:
        case 'get':
            response = retrieve_messages(request)
        case 'post':
            response = post_message(request)
        case _:
            response = form_request_format_error_response(
                f'invalid action is passed ({_})')
    return response


def retrieve_messages(request: dict) -> dict:
    response = {}
    try:
        n = request['last']
        if n > data['last']:
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
                            data['messages']
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


def post_message(request: dict) -> dict:
    response = {}
    try:
        nick = request['nick']
        text = request['text']
        data['last'] += 1
        data['messages'].append(Message(nick, data['last'], text))
        response = {
            'status': 'ok',
            'timeout': 1000
        }
    except KeyError as e:
        response = form_no_field_in_request_error_response(e.args[0])
    return response
    

async def main():
    server = await start_server(
        handle_connection,
        HOST,
        PORT
    )

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    run(main())

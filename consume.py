
def serve_request(message):
    print(message)
    print(dir(message))
    message.ack()
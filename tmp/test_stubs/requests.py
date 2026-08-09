class _Response:
    content = b""
    def raise_for_status(self): pass

def get(*args, **kwargs):
    return _Response()


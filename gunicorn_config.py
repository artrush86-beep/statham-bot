workers = 1
threads = 4
timeout = 120


def post_fork(server, worker):
    import app as application

    application.start_background_threads()

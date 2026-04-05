# gunicorn_config.py
workers = 1
threads = 4
timeout = 120

def post_fork(server, worker):
    """Перезапускаем фоновые потоки после fork в worker-процессе."""
    import threading
    import app as application

    threading.Thread(
        target=application._queue_worker,
        daemon=True, name="queue_worker"
    ).start()

    threading.Thread(
        target=application._scheduler,
        daemon=True, name="scheduler"
    ).start()

    if application.BYBIT_AVAILABLE or application.BINGX_AVAILABLE:
        threading.Thread(
            target=application._position_manager,
            daemon=True, name="pos_mgr"
        ).start()

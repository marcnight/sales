import logging

def setup_logging():
    """Configura o logging e retorna o logger"""
    logging.basicConfig(filename='app.log', level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger()
    logger.info('\n Inicio do processo')
    logger.info("Logging configurado.")
    return logger

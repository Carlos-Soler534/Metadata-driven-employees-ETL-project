import logging
import os

def get_logger(name=__name__):
    """
    Crea o devuelve un logger con configuración estándar (console y archivo).
    Asegura que no se dupliquen handlers si ya existe el logger.
    
    Args:
        name (str): nombre del logger (usualmente el nombre del módulo o clase)

    Returns:
        logging.Logger: instancia de logger configurado
    """

    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Ruta absoluta al archivo de log dentro de la carpeta utils
        current_dir = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(current_dir, "etl_pipeline.log")

        # Asegura que la carpeta exista
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


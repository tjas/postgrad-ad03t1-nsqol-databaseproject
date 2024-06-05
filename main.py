# -*- coding: utf-8 -*-

#import os
import random
import pymongo
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import time
from datetime import datetime
from faker import Faker
#from bson.objectid import ObjectId
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Tuple, Any, Dict
import platform
import psutil
import matplotlib.pyplot as plt
from tqdm import tqdm


__author__      = "Thiago Jorge Almeida dos Santos"
__copyright__   = "Copyright 2024, Thiago Jorge Almeida dos Santos"
__credits__     = ["Thiago Jorge Almeida dos Santos"]
__maintainer__  = ["Thiago Jorge Almeida dos Santos"]
__email__       = "thiago.tjas@gmail.com"
__version__     = "1.0.20240602"
__license__     = "proprietary"
__status__      = "development"


#################################
# Configurações gerais iniciais #
#################################

# Configurar Faker
fake = Faker()

# Configurar Logging
logging.basicConfig(filename='simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S.%f',
                    handlers=[RotatingFileHandler('simulation.log', maxBytes=5*1024*1024, backupCount=1)])


# Função para centralizar informações gerais
def log_system_info() -> None:
    """Log general information about the system and the simulation parameters."""
    system_info = {
        'Datetime': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'OS': platform.system(),
        'OS Version': platform.version(),
        'CPU': platform.processor(),
        'CPU Cores': multiprocessing.cpu_count(),
        'RAM': f"{psutil.virtual_memory().total / (1024 ** 3):.2f} GB",
        'RAM Available': f"{psutil.virtual_memory().available / (1024 ** 3):.2f} GB"
    }
    for key, value in system_info.items():
        logging.info(f"{key}: {value}")
        print(f"{key}: {value}")


############
# Conexões #
############

# Configurar MongoDB
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'inventory_db'
COLLECTIONS = ['stores', 'products', 'sales']

# Configurar MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['inventory_db']
stores_collection = db['stores']
products_collection = db['products']
sales_collection = db['sales']


def check_and_create_db(uri: str, db_name: str, collections: list) -> None:
    """Check if the MongoDB database and collections exist, and create them if they don't.

    Args:
        uri (str): The MongoDB URI.
        db_name (str): The name of the database.
        collections (list): A list of collection names to check/create.
    """
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Test the connection
        client.admin.command('ping')
        logging.info(f"Connected to MongoDB ({MONGO_URI}) successfully.")

        db = client[db_name]

        existing_collections = db.list_collection_names()
        for collection in collections:
            if collection not in existing_collections:
                db.create_collection(collection)
                logging.info(f"Collection '{collection}' created in database '{db_name}'.")
            else:
                logging.info(f"Collection '{collection}' already exists in database '{db_name}'.")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logging.error(f"Failed to connect to MongoDB ({MONGO_URI}): {e}")


####################
# Geração de dados #
####################

# Função para gerar produtos aleatórios
def generate_fake_product() -> Dict[str, Any]:
    """Generate a fake product with random attributes.

    Returns:
        Dict[str, Any]: A dictionary representing a fake product.
    """
    return {
        'product_id': fake.unique.uuid4(),
        'product_name': fake.word(),
        'category': fake.word(),
        'description': fake.text(max_nb_chars=200),
        'price': round(random.uniform(5.0, 500.0), 2),
        'stock_quantity': random.randint(0, 1000),
        'manufacturer': fake.company(),
        'sku': fake.unique.ean13(),
        'expiry_date': fake.date_between(start_date='today', end_date='+2y'),
        'supplier': fake.company()
    }


# Função para gerar filiais aleatórias
def generate_fake_store(min_products: int, max_products: int) -> Dict[str, Any]:
    """Generate a fake store with random attributes and products.

    Args:
        min_products (int): The minimum number of products in the store.
        max_products (int): The maximum number of products in the store.

    Returns:
        Dict[str, Any]: A dictionary representing a fake store.
    """
    store_id = fake.unique.uuid4()
    return {
        'store_id': store_id,
        'store_name': fake.company(),
        'address': fake.address(),
        'phone': fake.phone_number(),
        'manager_name': fake.name(),
        'email': fake.company_email(),
        'opening_date': fake.date_between(start_date='-10y', end_date='today'),
        'number_of_employees': random.randint(5, 50),
        'store_area': round(random.uniform(50.0, 500.0), 2),
        'products': [generate_fake_product() for _ in range(random.randint(min_products, max_products))]
    }


# Função para gerar vendas aleatórias
def generate_fake_sale(store_id: str, product_id: str) -> Dict[str, Any]:
    """Generate a fake sale with random attributes.

    Args:
        store_id (str): The ID of the store.
        product_id (str): The ID of the product.

    Returns:
        Dict[str, Any]: A dictionary representing a fake sale.
    """
    return {
        'sale_id': fake.unique.uuid4(),
        'store_id': store_id,
        'product_id': product_id,
        'quantity_sold': random.randint(1, 10),
        'sale_date': fake.date_time_this_year(),
        'customer_id': fake.unique.uuid4(),
        'customer_name': fake.name(),
        'payment_method': random.choice(['Credit Card', 'Cash', 'Debit Card']),
        'total_amount': round(random.uniform(10.0, 1000.0), 2),
        'items': [fake.word() for _ in range(random.randint(1, 5))]
    }


#####################
# Inserção de dados #
#####################

# Inserir dados de filiais no MongoDB
def insert_stores(num_stores: int, min_products: int, max_products: int) -> List[Dict[str, Any]]:
    """Insert fake store data into the MongoDB.

    Args:
        num_stores (int): The number of stores to generate and insert.
        min_products (int): The minimum number of products in a store.
        max_products (int): The maximum number of products in a store.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries representing the stores inserted.
    """
    stores = [generate_fake_store(min_products, max_products) for _ in range(num_stores)]
    stores_collection.insert_many(stores)
    return stores


# Inserir dados de vendas no MongoDB
def insert_sales(num_sales: int, stores: List[Dict[str, Any]]) -> None:
    """Insert fake sales data into the MongoDB.

    Args:
        num_sales (int): The number of sales to generate and insert.
        stores (List[Dict[str, Any]]): A list of stores to associate sales with.
    """
    sales = []
    for _ in range(num_sales):
        store = random.choice(stores)
        product = random.choice(store['products'])
        sale = generate_fake_sale(store['store_id'], product['product_id'])
        sales.append(sale)
        # Atualizar estoque do produto vendido
        products_collection.update_one(
            {'product_id': product['product_id'], 'store_id': store['store_id']},
            {'$inc': {'stock_quantity': -sale['quantity_sold']}}
        )
    sales_collection.insert_many(sales)


#############
# Operações #
#############

# Função para consultar estoque
def query_stock(store_id: str) -> Tuple[List[Dict[str, Any]], float]:
    """Query the stock of a specific store.

    Args:
        store_id (str): The ID of the store to query.

    Returns:
        Tuple[List[Dict[str, Any]], float]: A list of products in the store and the query execution time.
    """
    start_time = time.time()
    products = products_collection.find({'store_id': store_id})
    result = list(products)
    end_time = time.time()
    return result, end_time - start_time


# Função para atualizar inventário
def update_inventory(store_id: str, product_id: str, quantity: int) -> float:
    """Update the inventory of a specific product in a store.

    Args:
        store_id (str): The ID of the store.
        product_id (str): The ID of the product.
        quantity (int): The quantity to update the stock by.

    Returns:
        float: The execution time of the update operation.
    """
    start_time = time.time()
    products_collection.update_one(
        {'product_id': product_id, 'store_id': store_id},
        {'$inc': {'stock_quantity': quantity}}
    )
    end_time = time.time()
    return end_time - start_time


# Função para adicionar uma nova filial
def add_store() -> None:
    """Add a new store to the database with a 10% chance."""
    if random.random() < 0.1:  # 10% de chance de adicionar uma nova filial
        store = generate_fake_store(min_products=5, max_products=20)
        stores_collection.insert_one(store)
    

# Função para adicionar um novo produto
def add_product(store_id: str) -> None:
    """Add a new product to a store with a 10% chance.

    Args:
        store_id (str): The ID of the store to add the product to.
    """
    if random.random() < 0.1:  # 10% de chance de adicionar um novo produto
        new_product = generate_fake_product()
        products_collection.insert_one({**new_product, 'store_id': store_id})


##############
# Simulações #
##############

# Obter o número de núcleos do processador
def get_num_cores() -> int:
    """Get the number of CPU cores available on the machine.

    Returns:
        int: The number of CPU cores.
    """
    return multiprocessing.cpu_count()


# Simular consultas simultâneas
def simulate_operations(num_operations: int, stores: List[Dict[str, Any]], percent_cores: float) -> Tuple[List[float], List[float], Dict[str, int]]:
    """Simulate concurrent operations on the database.

    Args:
        num_operations (int): The number of operations to simulate.
        stores (List[Dict[str, Any]]): A list of stores to use in the simulation.
        percent_cores (float): The percentage of CPU cores to use for the simulation.

    Returns:
        Tuple[List[float], List[float], Dict[str, int]]: Lists of read and write times, and a count of each operation type.
    """
    num_cores = get_num_cores()
    max_workers = max(1, int(num_cores * percent_cores))

    read_times: List[float] = []
    write_times: List[float] = []
    operation_counts: Dict[str, int] = {
        'query_stock': 0,
        'update_inventory': 0,
        'add_store': 0,
        'add_product': 0
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _ in range(num_operations):
            store = random.choice(stores)
            operation = random.choice(['query_stock', 'update_inventory', 'add_store', 'add_product'])
            if operation == 'query_stock':
                futures.append(executor.submit(query_stock, store['store_id']))
            elif operation == 'update_inventory':
                product = random.choice(store['products'])
                futures.append(executor.submit(update_inventory, store['store_id'], product['product_id'], random.randint(1, 10)))
            elif operation == 'add_store':
                futures.append(executor.submit(add_store))
            elif operation == 'add_product':
                futures.append(executor.submit(add_product, store['store_id']))

        for future in tqdm(as_completed(futures), total=num_operations, desc="Operations Progress"):
            try:
                result = future.result()
                if isinstance(result, tuple):
                    read_times.append(result[1])
                    operation_counts['query_stock'] += 1
                else:
                    write_times.append(result)
                    if 'update_inventory' in future._fn.__name__:
                        operation_counts['update_inventory'] += 1
                    elif 'add_store' in future._fn.__name__:
                        operation_counts['add_store'] += 1
                    elif 'add_product' in future._fn.__name__:
                        operation_counts['add_product'] += 1
            except Exception as e:
                logging.error(f"Error: {e}")
    
    return read_times, write_times, operation_counts


# Registrar desempenho
def measure_performance(runs: int = 10, num_operations: int = 1000, percent_cores: float = 0.5, num_sales: int = 50, num_stores: int = 5, min_products: int = 5, max_products: int = 20) -> None:
    """Measure the performance of the simulation over multiple runs.

    Args:
        runs (int): The number of runs to execute. Default is 10.
        num_operations (int): The number of operations per simulation. Default is 1000.
        percent_cores (float): The percentage of CPU cores to use. Default is 0.5.
        num_sales (int): The number of initial sales to insert. Default is 50.
        num_stores (int): The number of initial stores to insert. Default is 5.
        min_products (int): The minimum number of products in a store. Default is 5.
        max_products (int): The maximum number of products in a store. Default is 20.
    """
    total_times: List[float] = []
    all_read_times: List[float] = []
    all_write_times: List[float] = []
    total_operations: Dict[str, int] = {
        'query_stock': 0,
        'update_inventory': 0,
        'add_store': 0,
        'add_product': 0
    }

    for run in tqdm(range(runs), desc="Simulation Runs"):
        start_time = time.time()
        stores = insert_stores(num_stores, min_products, max_products)
        insert_sales(num_sales, stores)
        read_times, write_times, operation_counts = simulate_operations(num_operations, stores, percent_cores)
        end_time = time.time()
        
        total_times.append(end_time - start_time)
        all_read_times.extend(read_times)
        all_write_times.extend(write_times)
        for key in total_operations:
            total_operations[key] += operation_counts[key]
        
        avg_total_time = sum(total_times) / len(total_times)
        avg_read_time = sum(all_read_times) / len(all_read_times)
        avg_write_time = sum(all_write_times) / len(all_write_times)
        
        logging.info(f"Run {run + 1} - Total execution time: {end_time - start_time:.4f} seconds")
        logging.info(f"Run {run + 1} - Number of query_stock: {operation_counts['query_stock']}")
        logging.info(f"Run {run + 1} - Number of update_inventory: {operation_counts['update_inventory']}")
        logging.info(f"Run {run + 1} - Number of add_store: {operation_counts['add_store']}")
        logging.info(f"Run {run + 1} - Number of add_product: {operation_counts['add_product']}")
        logging.info(f"Run {run + 1} - Average read time: {avg_read_time:.4f} seconds")
        logging.info(f"Run {run + 1} - Average write time: {avg_write_time:.4f} seconds")

    final_avg_total_time = sum(total_times) / len(total_times)
    final_avg_read_time = sum(all_read_times) / len(all_read_times)
    final_avg_write_time = sum(all_write_times) / len(all_write_times)
    
    logging.info(f"Final Average total execution time: {final_avg_total_time:.4f} seconds")
    logging.info(f"Final Average read time: {final_avg_read_time:.4f} seconds")
    logging.info(f"Final Average write time: {final_avg_write_time:.4f} seconds")
    logging.info(f"Total Number of query_stock: {total_operations['query_stock']}")
    logging.info(f"Total Number of update_inventory: {total_operations['update_inventory']}")
    logging.info(f"Total Number of add_store: {total_operations['add_store']}")
    logging.info(f"Total Number of add_product: {total_operations['add_product']}")

    # Plot individual run times
    for run, (total_time, read_time, write_time) in enumerate(zip(total_times, all_read_times, all_write_times)):
        plt.figure()
        plt.bar(['Total Time', 'Read Time', 'Write Time'], [total_time, read_time, write_time])
        plt.title(f'Performance for Run {run + 1}')
        plt.ylabel('Time (seconds)')
        plt.savefig(f'performance_run_{run + 1}.png')
        plt.close()

    # Plot aggregate run times
    plt.figure()
    plt.bar(['Total Time', 'Read Time', 'Write Time'], [final_avg_total_time, final_avg_read_time, final_avg_write_time])
    plt.title('Aggregate Performance Across Runs')
    plt.ylabel('Time (seconds)')
    plt.savefig('aggregate_performance.png')
    plt.close()


#############
# Principal #
#############

if __name__ == '__main__':
    log_system_info()
    check_and_create_db(MONGO_URI, DB_NAME, COLLECTIONS)
    measure_performance()
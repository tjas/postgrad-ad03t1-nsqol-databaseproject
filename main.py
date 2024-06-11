# -*- coding: utf-8 -*-

import os
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
#import numpy as np
from tqdm import tqdm
import numpy as np
import pandas as pd
#import mplfinance as mpf


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
#logging.basicConfig(filename='simulation.log', level=logging.INFO,
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s.%(msecs)03d %(process)5s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[RotatingFileHandler('simulation.log', maxBytes=5*1024*1024, backupCount=1)])

# Função para centralizar informações gerais
def log_system_info() -> None:
    """Log general information about the system and the simulation parameters."""
    system_info = {
        'Datetime': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'OS/Version': f"{platform.system()} / {platform.version()}",
        'CPU/Cores': f"{platform.processor()} / {multiprocessing.cpu_count()}",
        'RAM/Available': f"{psutil.virtual_memory().total / (1024 ** 3):.2f} GB / {psutil.virtual_memory().available / (1024 ** 3):.2f} GB",
    }
    for key, value in system_info.items():
        logging.info(f"{key}: {value}")
        print(f"{key}: {value}")


############
# Conexões #
############

# Configurar MongoDB
MONGO_HOST = 'localhost'
MONGO_PORT = 27013
DB_NAME = 'inventory_db'
COLLECTIONS = ['stores', 'products', 'sales']
USER = 'admin'
PASS = 'admin'


db = None
stores_collection = None
products_collection = None
sales_collection = None


def check_and_create_db(host: str, port: int, db_name: str, username: str = 'admin', password: str = 'admin', collections: list = []) -> Any:
    """Check if the MongoDB database and collections exist, and create them if they don't.

    Args:
        host (str): The MongoDB host.
        port (str): The MongoDB port.
        db_name (str): The name of the database.
        username (str, optional): The username for authentication. Defaults to 'admin'.
        password (str, optional): The password for authentication. Defaults to 'admin'.
        collections (list, optional): A list of collection names to check/create. Defaults to an empty list.
    """
    uri = f'mongodb://{username}:{password}@{host}'
    db = None
    try:
        #client = pymongo.MongoClient(uri, port, serverSelectionTimeoutMS=5000)
        client = pymongo.MongoClient(uri, port)
        # Test the connection
        client.admin.command('ping')
        logging.info(f"Connected to MongoDB ({MONGO_HOST}) successfully.")

        db = client[db_name]

        existing_collections = db.list_collection_names()
        for collection in collections:
            if collection not in existing_collections:
                db.create_collection(collection)
                logging.info(f"Collection '{collection}' created in database '{db_name}'.")
            else:
                logging.info(f"Collection '{collection}' already exists in database '{db_name}'.")
           
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        logging.error(f"Failed to connect to MongoDB ({MONGO_HOST}): {e}")
        
    return db
    


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
        'expiry_date': fake.date_time_between(start_date=datetime.now(), end_date='+2y'),
        #'expiry_date': datetime.strptime(fake.date_between(start_date='today', end_date='+2y'), '%Y-%m-%d %H:%M:%S'),
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
    # store_id = fake.unique.uuid4()
    return {
        'store_id': fake.unique.uuid4(),
        'store_name': fake.company(),
        'address': fake.address(),
        'phone': fake.phone_number(),
        'manager_name': fake.name(),
        'email': fake.company_email(),
        #'opening_date': fake.date_time_between(start_date='-10y', end_date='today'),
        'opening_date': fake.date_time_between(start_date='-10y', end_date=datetime.now()),
        #'opening_date': datetime.strptime(fake.date_between(start_date='-10y', end_date='today'), '%Y-%m-%d %H:%M:%S'),
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
        #'sale_date': datetime.strptime(fake.date_time_this_year(), '%Y-%m-%d %H:%M:%S'),
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
def add_store() -> float:
    """Add a new store to the database with a 30% chance."""
    #if random.random() < 0.3:  # 30% de chance de adicionar uma nova filial
    start_time = time.time()
    store = generate_fake_store(min_products=5, max_products=20)
    stores_collection.insert_one(store)
    end_time = time.time()
    return end_time - start_time
    

# Função para adicionar um novo produto
def add_product(store_id: str) -> float:
    """Add a new product to a store with a 30% chance.

    Args:
        store_id (str): The ID of the store to add the product to.
    """
    #if random.random() < 0.3:  # 30% de chance de adicionar um novo produto
    start_time = time.time()
    new_product = generate_fake_product()
    products_collection.insert_one({**new_product, 'store_id': store_id})
    end_time = time.time()
    return end_time - start_time


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
def simulate_operations(num_operations: int, stores: List[Dict[str, Any]], percent_cores: float, run_number: int, output_folder: str) -> Tuple[List[float], List[float], Dict[str, int]]:
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
    logging.info(f"Number of cores to be used: {max_workers}")

    read_times: List[float] = []
    write_times: List[float] = []
    operation_counts: Dict[str, int] = {
        'query_stock': 0,
        'update_inventory': 0,
        'add_store': 0,
        'add_product': 0
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_operation = {}
        for _ in range(num_operations):
            store = random.choice(stores)
            operation = random.choice(['query_stock', 'update_inventory', 'add_store', 'add_product'])
            if operation == 'query_stock':
                future = executor.submit(query_stock, store['store_id'])
            elif operation == 'update_inventory':
                product = random.choice(store['products'])
                future = executor.submit(update_inventory, store['store_id'], product['product_id'], random.randint(1, 10))
            elif operation == 'add_store':
                future = executor.submit(add_store)
            elif operation == 'add_product':
                future = executor.submit(add_product, store['store_id'])
            future_to_operation[future] = operation

        for future in tqdm(as_completed(future_to_operation), total=num_operations, desc=f"Operations Progress {run_number}"):
            try:
                result = future.result()
                operation = future_to_operation[future]
                if operation == 'query_stock':
                    read_times.append(result[1] * 1000)  # Convert to milliseconds
                    operation_counts['query_stock'] += 1
                else:
                    write_times.append(result * 1000)  # Convert to milliseconds
                    if operation == 'update_inventory':
                        operation_counts['update_inventory'] += 1
                    elif operation == 'add_store':
                        operation_counts['add_store'] += 1
                    elif operation == 'add_product':
                        operation_counts['add_product'] += 1
            except Exception as e:
                logging.error(e)

    # Generate individual run plots
    plot_individual_times(run_number, read_times, write_times, output_folder)

    return read_times, write_times, operation_counts


#TIME_LABEL = 'Time (ms)'

def plot_individual_times(run: int, read_times: List[float], write_times: List[float], output_folder: str, chart_width: int = 600) -> None:
    # Scatter plot for read times
    plt.figure(figsize=(chart_width / 100, 6))
    plt.scatter(range(len(read_times)), read_times)
    plt.title(f'Read Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    plt.xticks(np.arange(0, len(read_times), max(1, len(read_times) // 10)))
    plt.yticks(np.arange(0, max(read_times) + 5, 5))
    plt.savefig(os.path.join(output_folder, f'read_times_run_{run + 1}.png'))
    plt.close()

    # Scatter plot for write times
    plt.figure(figsize=(chart_width / 100, 6))
    plt.scatter(range(len(write_times)), write_times)
    plt.title(f'Write Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    plt.xticks(np.arange(0, len(write_times), max(1, len(write_times) // 10)))
    plt.yticks(np.arange(0, max(write_times) + 5, 5))
    plt.savefig(os.path.join(output_folder, f'write_times_run_{run + 1}.png'))
    plt.close()

    # Scatter plot for total times
    total_times = [r + w for r, w in zip(read_times, write_times)]
    plt.figure(figsize=(chart_width / 100, 6))
    plt.scatter(range(len(total_times)), total_times)
    plt.title(f'Total Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    plt.xticks(np.arange(0, len(total_times), max(1, len(total_times) // 10)))
    plt.yticks(np.arange(0, max(total_times) + 5, 5))
    plt.savefig(os.path.join(output_folder, f'total_times_run_{run + 1}.png'))
    plt.close()

    # Bar chart with average line for read times
    avg_read_time = sum(read_times) / len(read_times) if read_times else 0
    plt.figure(figsize=(chart_width / 100, 6))
    plt.bar(range(len(read_times)), read_times)
    plt.axhline(y=avg_read_time, color='r', linestyle='-', label=f'Avg: {avg_read_time:.2f} ms')
    plt.title(f'Read Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max(read_times) + 5, 5))
    plt.xticks(np.arange(0, len(read_times), max(1, len(read_times) // 10)))
    # for i, v in enumerate(read_times):
    #     plt.text(i, v + 0.5, f"{v:.2f}", ha='center')
    plt.savefig(os.path.join(output_folder, f'read_bar_run_{run + 1}.png'))
    plt.close()

    # Bar chart with average line for write times
    avg_write_time = sum(write_times) / len(write_times) if write_times else 0
    plt.figure(figsize=(chart_width / 100, 6))
    plt.bar(range(len(write_times)), write_times)
    plt.axhline(y=avg_write_time, color='r', linestyle='-', label=f'Avg: {avg_write_time:.2f} ms')
    plt.title(f'Write Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max(write_times) + 5, 5))
    plt.xticks(np.arange(0, len(write_times), max(1, len(write_times) // 10)))
    # for i, v in enumerate(write_times):
    #     plt.text(i, v + 0.5, f"{v:.2f}", ha='center')
    plt.savefig(os.path.join(output_folder, f'write_bar_run_{run + 1}.png'))
    plt.close()

    # Bar chart with average line for total times
    avg_total_time = sum(total_times) / len(total_times) if total_times else 0
    plt.figure(figsize=(chart_width / 100, 6))
    plt.bar(range(len(total_times)), total_times)
    plt.axhline(y=avg_total_time, color='r', linestyle='-', label=f'Avg: {avg_total_time:.2f} ms')
    plt.title(f'Total Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max(total_times) + 5, 5))
    plt.xticks(np.arange(0, len(total_times), max(1, len(total_times) // 10)))
    # for i, v in enumerate(total_times):
    #     plt.text(i, v + 0.5, f"{v:.2f}", ha='center')
    plt.savefig(os.path.join(output_folder, f'total_bar_run_{run + 1}.png'))
    plt.close()
    
    
    # Encontre o tamanho máximo entre os dois arrays
    max_size = max(len(read_times), len(write_times))

    # Ajuste os arrays para terem o mesmo tamanho
    read_times_adjusted = np.pad(read_times, (0, max_size - len(read_times)), 'constant', constant_values=(0))
    write_times_adjusted = np.pad(write_times, (0, max_size - len(write_times)), 'constant', constant_values=(0))

    # Bar chart comparing read and write times for each run
    plt.figure(figsize=(chart_width / 100, 6))
    bar_width = 0.35
    # Agora, use read_times_adjusted e write_times_adjusted para o plot
    #index = np.arange(len(read_times))
    index = np.arange(max_size)
    # plt.bar(index, read_times, bar_width, label='Read Time')
    plt.bar(index, read_times_adjusted, bar_width, label='Read Time')
    # plt.bar(index + bar_width, write_times, bar_width, label='Write Time')
    plt.bar(index + bar_width, write_times_adjusted, bar_width, label='Write Time')
    plt.title(f'Read vs Write Times for Run {run + 1}')
    plt.xlabel('Operation')
    plt.ylabel('Time (ms)')
    # Ajuste plt.xticks para corresponder ao número de operações
    # plt.xticks(index + bar_width / 2, np.arange(0, len(read_times), max(1, len(read_times) // 10)))
    plt.xticks(index + bar_width / 2, np.arange(max_size))
    plt.yticks(np.arange(0, max(read_times + write_times) + 5, 5))
    plt.legend()
    plt.savefig(os.path.join(output_folder, f'read_vs_write_run_{run + 1}.png'))
    plt.close()
    

# Registrar desempenho
def measure_performance(runs: int = 10, num_operations: int = 1000, percent_cores: float = 0.5, num_sales: int = 50, num_stores: int = 5, min_products: int = 5, max_products: int = 20, chart_width: int = 600) -> None:
    total_times: List[float] = []
    all_read_times: List[List[float]] = []
    all_write_times: List[List[float]] = []
    total_operations: Dict[str, int] = {
        'query_stock': 0,
        'update_inventory': 0,
        'add_store': 0,
        'add_product': 0
    }

    # Configurações de simulação
    num_cores = get_num_cores()
    max_workers = max(1, int(num_cores * percent_cores))
    
    logging.info(f"Starting simulation with {runs} runs and {num_operations} operations per run.")
    logging.info(f"Using {max_workers} out of {num_cores} cores.")

    print(f"Starting simulation with {runs} runs and {num_operations} operations per run.")
    print(f"Using {max_workers} out of {num_cores} cores.")

    # Criar diretório para salvar gráficos e logs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_folder = os.path.join("executions", timestamp)
    os.makedirs(output_folder, exist_ok=True)

    log_file = os.path.join(output_folder, 'simulation_log.txt')
    file_handler = logging.FileHandler(log_file)
    logging.getLogger().addHandler(file_handler)

    # for run in tqdm(range(runs), desc="Simulation Runs"):
    for run in range(runs):
        start_time = time.time()
        stores = insert_stores(num_stores, min_products, max_products)
        insert_sales(num_sales, stores)
        read_times, write_times, operation_counts = simulate_operations(num_operations, stores, percent_cores, run, output_folder)
        end_time = time.time()

        total_times.append((end_time - start_time) * 1000)  # Convert to milliseconds
        all_read_times.append(read_times)
        all_write_times.append(write_times)
        for key in total_operations:
            total_operations[key] += operation_counts[key]
            
        #print("\n\n", all_read_times, "\n\n")

        # avg_read_time = sum(all_read_times) / len(all_read_times) if all_read_times else 0
        # avg_write_time = sum(all_write_times) / len(all_write_times) if all_write_times else 0
        # avg_total_time = sum(total_times) / len(total_times) if total_times else 0
        
        avg_read_time = sum([sum(rt) for rt in all_read_times]) / len(all_read_times) if all_read_times else 0
        avg_write_time = sum([sum(wt) for wt in all_write_times]) / len(all_write_times) if all_write_times else 0
        avg_total_time = sum(total_times) / len(total_times) if total_times else 0

        logging.info(f"Run {run + 1} - Total execution time: {(end_time - start_time) * 1000:.4f} ms")
        logging.info(f"Run {run + 1} - Number of query_stock: {operation_counts['query_stock']}")
        logging.info(f"Run {run + 1} - Number of update_inventory: {operation_counts['update_inventory']}")
        logging.info(f"Run {run + 1} - Number of add_store: {operation_counts['add_store']}")
        logging.info(f"Run {run + 1} - Number of add_product: {operation_counts['add_product']}")
        logging.info(f"Run {run + 1} - Average read time: {avg_read_time:.4f} ms")
        logging.info(f"Run {run + 1} - Average write time: {avg_write_time:.4f} ms")

    # final_avg_total_time = sum(total_times) / len(total_times) if total_times else 0
    # final_avg_read_time = sum(all_read_times) / len(all_read_times) if all_read_times else 0
    # final_avg_write_time = sum(all_write_times) / len(all_write_times) if all_write_times else 0
    
    final_avg_total_time = sum(total_times) / len(total_times) if total_times else 0
    final_avg_read_time = sum([sum(rt) for rt in all_read_times]) / len(all_read_times) if all_read_times else 0
    final_avg_write_time = sum([sum(wt) for wt in all_write_times]) / len(all_write_times) if all_write_times else 0

    logging.info(f"Final Average total execution time: {final_avg_total_time:.4f} ms")
    logging.info(f"Final Average read time: {final_avg_read_time:.4f} ms")
    logging.info(f"Final Average write time: {final_avg_write_time:.4f} ms")
    logging.info(f"Total Number of query_stock: {total_operations['query_stock']}")
    logging.info(f"Total Number of update_inventory: {total_operations['update_inventory']}")
    logging.info(f"Total Number of add_store: {total_operations['add_store']}")
    logging.info(f"Total Number of add_product: {total_operations['add_product']}")
    
    # Flatten the lists for scatter plots
    # flat_all_read_times = [time for sublist in all_read_times for time in sublist]
    # flat_all_write_times = [time for sublist in all_write_times for time in sublist]
    flat_all_read_times = [sum(itens) for itens in all_read_times]
    flat_all_write_times = [sum(itens) for itens in all_write_times]

    # Max values and subdivisions for vertical and horizontal axes
    # max_read_time = max(all_read_times) if all_read_times else 0
    max_read_time = max(flat_all_read_times) if flat_all_read_times else 0
    # max_write_time = max(all_write_times) if all_write_times else 0
    max_write_time = max(flat_all_write_times) if flat_all_write_times else 0
    max_total_time = max(total_times) if total_times else 0
    max_operations = max(num_operations, runs)

    # Scatter plot for total read times across all runs
    plt.figure(figsize=(chart_width / 100, 6))
    # plt.scatter(range(len(all_read_times)), all_read_times)
    plt.scatter(range(len(flat_all_read_times)), flat_all_read_times)
    plt.axhline(y=final_avg_read_time, color='r', linestyle='-', label=f'Avg: {final_avg_read_time:.2f} ms')
    plt.title('Total Read Times Across All Runs')
    plt.xlabel('Operation')
    plt.ylabel('Read Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max_read_time + 5, 5))
    # plt.yticks(np.arange(0, max(max_read_time) + 5, 5))
    # plt.yticks(np.arange(0, max_read_time, 5))
    plt.xticks(np.arange(0, max_operations, max(10, max_operations // 10)))
    plt.savefig(os.path.join(output_folder, 'total_read_times_all_runs.png'))
    plt.close()

    # Scatter plot for total write times across all runs
    plt.figure(figsize=(chart_width / 100, 6))
    # plt.scatter(range(len(all_write_times)), all_write_times)
    plt.scatter(range(len(flat_all_write_times)), flat_all_write_times)
    plt.axhline(y=final_avg_write_time, color='r', linestyle='-', label=f'Avg: {final_avg_write_time:.2f} ms')
    plt.title('Total Write Times Across All Runs')
    plt.xlabel('Operation')
    plt.ylabel('Write Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max_write_time + 5, 5))
    # plt.yticks(np.arange(0, max(max_write_time) + 5, 5))
    plt.xticks(np.arange(0, max_operations, max(10, max_operations // 10)))
    plt.savefig(os.path.join(output_folder, 'total_write_times_all_runs.png'))
    plt.close()

    # Scatter plot for total execution times across all runs
    plt.figure(figsize=(chart_width / 100, 6))
    plt.scatter(range(len(total_times)), total_times)
    plt.axhline(y=final_avg_total_time, color='r', linestyle='-', label=f'Avg: {final_avg_total_time:.2f} ms')
    plt.title('Total Execution Times Across All Runs')
    plt.xlabel('Operation')
    plt.ylabel('Total Execution Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max_total_time + 5, 5))
    plt.xticks(np.arange(0, max_operations, max(10, max_operations // 10)))
    plt.savefig(os.path.join(output_folder, 'total_execution_times_all_runs.png'))
    plt.close()

    # Bar chart with average line for read times across all runs
    plt.figure(figsize=(chart_width / 100, 6))
    # plt.bar(range(len(all_read_times)), all_read_times)
    plt.bar(range(len(flat_all_read_times)), flat_all_read_times)
    plt.axhline(y=final_avg_read_time, color='r', linestyle='-', label=f'Avg: {final_avg_read_time:.2f} ms')
    plt.title('Read Times Across All Runs')
    plt.xlabel('Operation')
    plt.ylabel('Read Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max_read_time + 5, 5))
    # plt.yticks(np.arange(0, max(max_read_time) + 5, 5))
    plt.xticks(np.arange(0, max_operations, max(10, max_operations // 10)))
    # for i, v in enumerate(all_read_times):
    # for i, v in enumerate(flat_all_read_times):
    #     plt.text(i, v + 0.5, f"{v:.2f}", ha='center')
    plt.savefig(os.path.join(output_folder, 'read_times_bar_all_runs.png'))
    plt.close()

    # Bar chart with average line for write times across all runs
    plt.figure(figsize=(chart_width / 100, 6))
    # plt.bar(range(len(all_write_times)), all_write_times)
    plt.bar(range(len(flat_all_write_times)), flat_all_write_times)
    plt.axhline(y=final_avg_write_time, color='r', linestyle='-', label=f'Avg: {final_avg_write_time:.2f} ms')
    plt.title('Write Times Across All Runs')
    plt.xlabel('Operation')
    plt.ylabel('Write Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max_write_time + 5, 5))
    # plt.yticks(np.arange(0, max(max_write_time) + 5, 5))
    plt.xticks(np.arange(0, max_operations, max(10, max_operations // 10)))
    # for i, v in enumerate(all_write_times):
    # for i, v in enumerate(flat_all_write_times):
    #     plt.text(i, v + 0.5, f"{v:.2f}", ha='center')
    plt.savefig(os.path.join(output_folder, 'write_times_bar_all_runs.png'))
    plt.close()

    # Bar chart with average line for total times across all runs
    plt.figure(figsize=(chart_width / 100, 6))
    plt.bar(range(len(total_times)), total_times)
    plt.axhline(y=final_avg_total_time, color='r', linestyle='-', label=f'Avg: {final_avg_total_time:.2f} ms')
    plt.title('Total Execution Times Across All Runs')
    plt.xlabel('Operation')
    plt.ylabel('Total Execution Time (ms)')
    plt.legend()
    plt.yticks(np.arange(0, max_total_time + 5, 5))
    plt.xticks(np.arange(0, max_operations, max(10, max_operations // 10)))
    # for i, v in enumerate(total_times):
    #     plt.text(i, v + 0.5, f"{v:.2f}", ha='center')
    plt.savefig(os.path.join(output_folder, 'total_execution_times_bar_all_runs.png'))
    plt.close()


    # max_read_time = max(max(sublist) for sublist in all_read_times if sublist)
    # max_write_time = max(max(sublist) for sublist in all_write_times if sublist)
    
    # Bar chart comparing read and write times for each run
    plt.figure(figsize=(chart_width / 100, 6))
    bar_width = 0.35
    index = np.arange(runs)
    plt.bar(index, [sum(read_times) for read_times in all_read_times], bar_width, label='Read Time')
    plt.bar(index + bar_width, [sum(write_times) for write_times in all_write_times], bar_width, label='Write Time')
    plt.title('Read vs Write Times for Each Run')
    plt.xlabel('Run')
    plt.ylabel('Time (ms)')
    plt.xticks(index + bar_width / 2, np.arange(runs))
    #plt.yticks(np.arange(0, max(max(all_read_times), max(all_write_times)) + 250, 250))
    plt.yticks(np.arange(0, max(max(flat_all_read_times), max(flat_all_write_times)) + 250, 250))
    # plt.yticks(np.arange(0, max(max_read_time, max_write_time) + 250, 250))
    plt.legend()
    plt.savefig(os.path.join(output_folder, 'read_vs_write_each_run.png'))
    plt.close()

    # Candlestick chart comparing read and write times for each run
    # df = pd.DataFrame({
    #     'read_times': [sum(read_times) for read_times in all_read_times],
    #     'write_times': [sum(write_times) for write_times in all_write_times]
    # })
    # df['run'] = np.arange(runs)
    # # df.set_index('run', inplace=True)

    # # mpf.plot(
    # #     df,
    # #     type='candle',
    # #     style='charles',
    # #     title='Candlestick Chart of Read vs Write Times for Each Run',
    # #     ylabel='Time (ms)',
    # #     savefig=os.path.join(output_folder, 'candlestick_read_vs_write_each_run.png')
    # # )
    # # Configurando o gráfico
    # fig, ax = plt.subplots()

    # # Para cada run, desenhe um candlestick
    # for i in range(len(df)):
    #     # Definindo cores para os candlesticks baseado na comparação entre leitura e escrita
    #     color = 'green' if df.loc[i, 'read_times'] <= df.loc[i, 'write_times'] else 'red'
        
    #     # Desenhando o corpo do candlestick
    #     ax.plot([i, i], [df.loc[i, 'read_times'], df.loc[i, 'write_times']], color=color, lw=2)
        
    #     # Aqui você adicionaria o código para desenhar as sombras dos candlesticks
    #     # Como não temos dados de máximo e mínimo, isso será omitido

    # # Configurações adicionais do gráfico
    # ax.set_title('Candlestick Chart of Read vs Write Times for Each Run')
    # ax.set_ylabel('Time (ms)')
    # ax.set_xticks(range(len(df)))
    # ax.set_xticklabels(df['run'])

    # # Salvando o gráfico
    # plt.savefig(os.path.join(output_folder, 'candlestick_read_vs_write_each_run_matplotlib.png'))
    
    # Convert times to DataFrame
    df = pd.DataFrame({
        'run': np.arange(runs),
        'read_times': [sum(rt) for rt in all_read_times],
        'write_times': [sum(wt) for wt in all_write_times]
    })
    df['total_times'] = df['read_times'] + df['write_times']

    # Generate candlestick chart
    fig, ax = plt.subplots(figsize=(chart_width / 100, 6))
    for idx, row in df.iterrows():
        ax.plot([row['run'], row['run']], [row['write_times'], row['read_times']], color='black')
        ax.plot(row['run'], row['write_times'], marker='o', color='red')
        ax.plot(row['run'], row['read_times'], marker='o', color='green')
    ax.set_title('Candlestick Chart of Read vs Write Times for Each Run')
    ax.set_xlabel('Run')
    ax.set_ylabel('Time (ms)')
    ax.set_xticks(np.arange(0, runs, max(1, runs // 10)))
    ax.set_yticks(np.arange(0, max(max_read_time, max_write_time) + 5, 5))
    #fig.legend(['Write Time', 'Read Time'])
    plt.savefig(os.path.join(output_folder, 'candlestick_read_vs_write_each_run.png'))
    plt.close()



#############
# Principal #
#############

if __name__ == '__main__':
    log_system_info()
    db = check_and_create_db(MONGO_HOST, MONGO_PORT, DB_NAME, USER, PASS, COLLECTIONS)
    #print('', db)
    stores_collection = db['stores']
    products_collection = db['products']
    sales_collection = db['sales']
    measure_performance(10, 100)
    
import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
import pymongo
import multiprocessing
from functools import partial
from category_links import *
from helper import *

#conectando ao mongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["atacadao_3"]
col = db["products"]

def scrape_category(url, initial_page, pages_to_scrape):
    ##  Extracts product names and links from a specific category and stores them in MongoDB. 
    ##  Receives the category's product URL, the page to start scraping, and the number of pages to traverse.
    
    #set the page number
    page = initial_page

    #set the number of pages to scrap
    pages = pages_to_scrape

    #loop tru all the pages
    while page <= int(pages):
        #open the url
        response = requests.get(url + "?p=" + str(page))

        #print open page
        print("Open page: " + url + "?p=" + str(page))
        response = requests.get(url)

        #criando o objeto soup
        soup = BeautifulSoup(response.content, 'html.parser')

        #pegando todos os products <li class="product-item not-logged">

        products = soup.find_all('li', class_='product-item not-logged')

        for product in products:
            name_element = product.find_all('a', class_='product-item-link')
            product_name = name_element[0].text.strip() if name_element else ""

            # Extrair o link do produto
            product_link = name_element[0]['href'] if name_element else ""

            # Extrair a marca
            brand_element = product.find_all('span', class_='brand')
            brand = brand_element[0].text.strip() if brand_element else ""

            # Extrair o tamanho
            size_element = product.find_all('span', class_='size')
            size = size_element[0].text.strip() if size_element else ""

            #adicinando os dados no mongoDB
            col.insert_one({
                'name': product_name,
                'link': product_link,
                'brand': brand,
                'size': size
            })

            print('Salvando: ', product_name)

def scrape_page(url):
    ## ## Extracts data from a specific product and saves it in MongoDB.
    response = requests.get(url)

    #print open page
    print("Open page: " + url)
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')


    products = soup.find_all('li', class_='product-item not-logged')

    for product in products:
        name_element = product.find_all('a', class_='product-item-link')
        product_name = name_element[0].text.strip() if name_element else ""

        product_link = name_element[0]['href'] if name_element else ""

        brand_element = product.find_all('span', class_='brand')
        brand = brand_element[0].text.strip() if brand_element else ""

        size_element = product.find_all('span', class_='size')
        size = size_element[0].text.strip() if size_element else ""

        col.insert_one({
            'name': product_name,
            'link': product_link,
            'brand': brand,
            'size': size
        })

        print('Salvando: ', product_name)

def scrape_page(url,category_name):
    ## Extracts information from a product and saves it along with the category name.
    response = requests.get(url)

    print("Open page: " + url)
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')


    products = soup.find_all('li', class_='product-item not-logged')

    for product in products:
        name_element = product.find_all('a', class_='product-item-link')
        product_name = name_element[0].text.strip() if name_element else ""

        product_link = name_element[0]['href'] if name_element else ""

        brand_element = product.find_all('span', class_='brand')
        brand = brand_element[0].text.strip() if brand_element else ""

        size_element = product.find_all('span', class_='size')
        size = size_element[0].text.strip() if size_element else ""

        #adicinando os dados no mongoDB
        col.insert_one({
            'nome': product_name,
            'link': product_link,
            'marca': brand,
            'category_name': category_name,
            'tamanho': size
        })

        print('Salvando: ', product_name)

def scrape_category(category_url, initial_page, pages_to_scrape, num_threads):
    ## Extracts product names and links from a specific category using multiple threads to speed up execution.
    url = category_url

    #set the page number
    page = initial_page

    #set the number of pages to scrap
    pages = pages_to_scrape

    #create a list with all page urls to be scraped
    page_urls = [url + "?p=" + str(page) for page in range(page, page + pages)]

    #create a thread pool
    pool = ThreadPool(num_threads)

    #scrape all pages using the thread pool
    pool.map(scrape_page, page_urls)

    #close the pool and wait for all threads to finish
    pool.close()
    pool.join()

def scrape_category(category_url, initial_page, pages_to_scrape, num_threads, category_name):
    ## Extracts product names and links from a specific category using multiple threads to speed up execution.
    url = category_url

    #set the page number
    page = initial_page

    #set the number of pages to scrap
    pages = pages_to_scrape

    #create a list with all page urls to be scraped
    page_urls = [url + "?p=" + str(page) for page in range(page, page + pages)]

    scrape_page_with_category = partial(scrape_page, category_name=category_name)

    #create a thread pool
    pool = ThreadPool(num_threads)

    #scrape all pages using the thread pool
    pool.map(scrape_page_with_category, page_urls)

    #close the pool and wait for all threads to finish
    pool.close()
    pool.join()
    
def extract_product_info(url):
    ## Extracts information from the product page.
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        image_url = soup.find('meta', property='og:image')['content']

        ean = soup.find('span', class_='ean').text.strip()

        cx_numbers = extract_cx_numbers(html)
        
        col.update_one({"link": url}, {"$set": {"images_url": image_url, "ean": ean, "cx_numbers": cx_numbers, "scraped": True}})
    
    else:
        return None

def scrape_all_products(num_threads):
    ## Iterates over all the products saved in MongoDB and extracts their information.

    #products = list(col.find({"scraped": {"$exists": False}}))
    products = list(col.find({}))
    num_products = len(products)

    with multiprocessing.Pool(processes=num_threads) as pool:
        for i, link in enumerate(pool.imap_unordered(extract_product_info, [prod["link"] for prod in products])):
            col.update_one({"link": link}, {"$set": {"scraped": True}})

            print(f"Product {i+1}/{num_products} raspado: {link}")

def save_to_csv():
    ## Save al the product information on a csv to import in WoCommerce
    products = list(col.find({"ean": {"$exists": True}}))
    
    with open("products.csv", "w", encoding="utf-8") as file:
        file.write("SKU;Name;Short Description;Description;Categories;Tags;Attribute 1 name;Attribute 1 value(s)\n")

        for product in products:
            sku = product["ean"]
            name = product["nome"] 
            short_description = product["nome"]
            description = product["nome"]
            categories =    product["category_name"] + ',' + "Marca > " + product["marca"] 
            tags = product["tamanho"] 
            attribute_1_name = "Quantidade Caixa"
            attribute_1_values = ",".join(product["cx_numbers"])
            print(attribute_1_values)

            if not sku or not name or not short_description or not description or not categories or not tags or not attribute_1_name or not attribute_1_values:
                continue

            file.write(f"{sku};{name};{short_description};{description};{categories};{tags};{attribute_1_name};{attribute_1_values}\n")

def download_images():
    ## Download all the products images 
    products = list(col.find({"ean": {"$exists": True}}))
    num_products = len(products)

    with multiprocessing.Pool(processes=20) as pool:
        for i, link in enumerate(pool.imap_unordered(download_image, [video["images_url"] for video in products])):
            print(f"Imagem {i+1}/{num_products} downloaded: {link}")


if __name__ == '__main__':
    multiprocessing.freeze_support()
    num_threads = 24
    categorys_links = categorys
    ##itera sobre as categorias e salva o nome e link dos products no banco de dados 
   
    #itera sobre todo o tamanho da lista de links
    """
    for i in range(len(categorys_links)):
        #checa se a categoria já foi raspada, vendo se a chave 'scraped' existe e se é True
        if 'scraped' in categorys_links[i] and categorys_links[i]['scraped'] == True:
            continue
        else:
        #scrape_category(categorys_links[i], 1, 1, num_threads, categorys_names[i])
            scrape_category(categorys_links[i]['link'], 1, categorys_links[i]['pages'], num_threads, categorys_links[i]['name'])
            #atualiza a categoria com a chave 'scraped' = True
            categorys_links[i]['scraped'] = True
    """
    ## Roda o scrapper em todos products do banco 
    #scrape_all_products(num_threads)
    ## Salva informações utilizadas em um cvs para importação
    #save_to_csv()
    ## Baixa as imagens dos products 
    #download_images()
    create_image_csv()
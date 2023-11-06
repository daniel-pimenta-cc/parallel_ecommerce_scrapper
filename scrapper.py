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
col = db["produtos"]

def scrape_category(url, initial_page, pages_to_scrape):
    ## Extrai o nome do produto e links de uma categoria especifica
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

        #pegando todos os produtos <li class="product-item not-logged">

        produtos = soup.find_all('li', class_='product-item not-logged')

        for product in produtos:
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
    ## Extrai os dados do produto
    response = requests.get(url)

    #print open page
    print("Open page: " + url)
    response = requests.get(url)

    #criando o objeto soup
    soup = BeautifulSoup(response.content, 'html.parser')

    #pegando todos os produtos <li class="product-item not-logged">

    produtos = soup.find_all('li', class_='product-item not-logged')

    for product in produtos:
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

def scrape_page(url,category_name):
    ## Extrai as informações do produto e salva o nome da categoria 
    response = requests.get(url)

    #print open page
    print("Open page: " + url)
    response = requests.get(url)

    #criando o objeto soup
    soup = BeautifulSoup(response.content, 'html.parser')

    #pegando todos os produtos <li class="product-item not-logged">

    produtos = soup.find_all('li', class_='product-item not-logged')

    for product in produtos:
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
            'nome': product_name,
            'link': product_link,
            'marca': brand,
            'category_name': category_name,
            'tamanho': size
        })

        print('Salvando: ', product_name)

def scrape_category(category_url, initial_page, pages_to_scrape, num_threads):
    ## Extrai o nome do produto e links de uma categoria especifica utilizando paralelismo para agilizar 
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
    ## Extrai o nome do produto e links e salva no bd junto com o nome de uma categoria especifica utilizando paralelismo para agilizar 
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
    # Extrai informações da pagina do produto
    response = requests.get(url)
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        # Extrair informações do produto
        image_url = soup.find('meta', property='og:image')['content']


        ean = soup.find('span', class_='ean').text.strip()

        # Extrair ocorrências únicas de CX-numero
        cx_numbers = extract_cx_numbers(html)

        
        #atualiza produto com as informações extraidas e como já foi raspado
        col.update_one({"link": url}, {"$set": {"images_url": image_url, "ean": ean, "cx_numbers": cx_numbers, "scraped": True}})
    
    else:
        return None

def scrape_all_products(num_threads):
    # pega todos os produtos não raspados
    #products = list(col.find({"scraped": {"$exists": False}}))
    #pega todos os produtos
    products = list(col.find({}))
    num_products = len(products)

    # inicia a execução das threads
    with multiprocessing.Pool(processes=num_threads) as pool:
        for i, link in enumerate(pool.imap_unordered(extract_product_info, [video["link"] for video in products])):
            # atualiza o campo "scraped" para o produto
            col.update_one({"link": link}, {"$set": {"scraped": True}})

            # imprime informações sobre o progresso
            print(f"Produto {i+1}/{num_products} raspado: {link}")

def save_to_csv():
    # pega todos os produtos que tem as informações completas
    products = list(col.find({"ean": {"$exists": True}}))
    
    # cria um arquivo csv
    with open("produtos.csv", "w", encoding="utf-8") as file:
        # escreve o cabeçalho
        file.write("SKU;Name;Short Description;Description;Categories;Tags;Attribute 1 name;Attribute 1 value(s)\n")

        # escreve os dados de cada produto
        for product in products:
            sku = product["ean"]
            name = product["nome"] 
            short_description = product["nome"]
            description = product["nome"]
            categories =    product["category_name"] + ',' + "Marca > " + product["marca"] 
            tags = product["tamanho"] 
            attribute_1_name = "Quantidade Caixa"
            #cria uma string com todos os cx_numbers separados por virgula
            attribute_1_values = ",".join(product["cx_numbers"])
            print(attribute_1_values)

            #Se algum protudo tiver qualquer um dos campos vazio, ele não é escrito no csv
            if not sku or not name or not short_description or not description or not categories or not tags or not attribute_1_name or not attribute_1_values:
                continue

            file.write(f"{sku};{name};{short_description};{description};{categories};{tags};{attribute_1_name};{attribute_1_values}\n")

def download_images():
    # pega todos os produtos que tem as informações completas
    products = list(col.find({"ean": {"$exists": True}}))
    num_products = len(products)

    # inicia a execução das threads
    with multiprocessing.Pool(processes=20) as pool:
        for i, link in enumerate(pool.imap_unordered(download_image, [video["images_url"] for video in products])):
            # imprime informações sobre o progresso
            print(f"Imagem {i+1}/{num_products} baixada: {link}")


if __name__ == '__main__':
    multiprocessing.freeze_support()
    num_threads = 24
    categorys_links = categorys
    ##itera sobre as categorias e salva o nome e link dos produtos no banco de dados 
   
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
    ## Roda o scrapper em todos produtos do banco 
    #scrape_all_products(num_threads)
    ## Salva informações utilizadas em um cvs para importação
    #save_to_csv()
    ## Baixa as imagens dos produtos 
    #download_images()
    create_image_csv()
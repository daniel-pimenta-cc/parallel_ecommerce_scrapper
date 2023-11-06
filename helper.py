import re
import requests

def download_image(image_url):
    #limpa a url original "https://www.vilanova.com.br/media/catalog/product/7/8/7896040707473_1_1.jpg?quality=80&bg-color=255,255,255&fit=bounds&height=265&width=265&canvas=265:265" para "https://www.vilanova.com.br/media/catalog/product/7/8/7896040707473_1_1.jpg"
    image_url = image_url.split("?")[0]

    #pega o nome da imagem "7896040707473.jpg"
    image_name = image_url.split("/")[-1]
    image_name = image_name.split("_")[0]

    #baixa a imagem
    response = requests.get(image_url)

    #salva a imagem
    with open(f"images/{image_name}.jpg", "wb") as file:
        file.write(response.content)

def create_image_csv():
    #cria um arquivo csv com SKU e link da imagem
    with open("images.csv", "w", encoding="utf-8") as file:
        # escreve o cabeçalho
        file.write("SKU,Image URL\n")

        # pega todos os produtos que tem as informações completas
        products = list(col.find({"ean": {"$exists": True}}))

        # escreve os dados de cada produto
        for product in products:
            sku = product["ean"]
            image_constant = "https://multiplicaatacado.com/wp-content/uploads/2023/11/"
            image_url = image_constant + sku + ".jpg"
            #Se algum protudo tiver qualquer um dos campos vazio, ele não é escrito no csv
            if not sku or not image_url:
                continue

            file.write(f"{sku},{image_url}\n")

def extract_cx_numbers(html):
    #Lê as opções de tamanhos dos produtos 
    pattern_1 = r"CX-(\d+)"
    pattern_2 = r"FD-(\d+)"
    matches_1 = re.findall(pattern_1, html)
    matches_2 = re.findall(pattern_2, html)
    matches = matches_1 + matches_2
    unique_numbers = list(set(matches))
    return unique_numbers

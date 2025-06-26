import sqlite3
import json
import os
import tempfile
import sys
import numpy as np
import zipfile

from PIL import Image
from collections import Counter


def zip_files(folder_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                zipf.write(file_path, filename)

def get_dominant_color_int(image_path):
    """
    Возвращает доминирующий цвет как signed 32-bit integer.
    Формат: 0xAARRGGBB → преобразуется в signed int.
    Если все пиксели прозрачные — возвращает 0.
    """
    img = Image.open(image_path).convert("RGBA")
    pixels = list(img.getdata())

    color_counter = Counter()

    for pixel in pixels:
        if pixel[3] == 0:  # Пропускаем прозрачные пиксели
            continue
        r, g, b, _ = pixel
        color_counter[(r, g, b)] += 1

    if not color_counter:
        return 0  # Нет непрозрачных пикселей

    dominant_rgb = color_counter.most_common(1)[0][0]
    r, g, b = dominant_rgb

    # ARGB → hex → int (A = FF)
    argb_hex = (0xFF << 24) | (r << 16) | (g << 8) | b

    # Конвертируем в signed 32-bit int
    if argb_hex >= 0x80000000:
        argb_hex -= 0x100000000

    return argb_hex

def average_color(image_path):
    img = Image.open(image_path).convert("RGBA")
    img_array = np.array(img)

    # Фильтруем прозрачные пиксели
    non_transparent = img_array[img_array[:, :, 3] > 0]

    if len(non_transparent) == 0:
        return None

    avg = np.mean(non_transparent[:, :3], axis=0).astype(int)
    return tuple(avg)


db_file = 'sync_db.sqlite'
output_zip = "output.zip"
argc = len(sys.argv)
if argc == 3:
    db_file = sys.argv[1]
    output_zip = sys.argv[2]
elif argc != 1:
    print("Usage: stocatima [db output]")
    sys.exit(1)

if not os.path.isfile(db_file):
    print(f"No db found: {db_file}")
    sys.exit(1)

# Подключение к базе данных
conn = sqlite3.connect(db_file)  
cursor = conn.cursor()

# Шаг 1: Получаем все loyalty card JSON'ы
collection_base = "/users/%/loyalty-cards/"
content_type = "application/x.stocard.loyaltycard+json"
output_folder = f"{tempfile.gettempdir()}/stocatima"

try:
    cursor.execute("""
        SELECT collection,id,content FROM synced_resources
        WHERE collection LIKE ? AND content_type = ?
    """, (collection_base, content_type))
except:
    print(f"db error: {db_file}")
    sys.exit(1)

rows = cursor.fetchall()
if not rows:
    print("Не найдены loyalty card записи")
    sys.exit(1)

processed_providers = set()  # Уже обработанные карты
provider_counter = 1  # Счётчик ID для записей

# Создаём выходную папку
os.makedirs(f"{output_folder}", exist_ok=True)

# Открываем файл для записи
with open(f"{output_folder}/catima.csv", "w", encoding="utf-8") as txt_file:

    txt_file.write("2\n\n_id\n\n" \
    "_id,store,note,validfrom,expiry,balance," \
    "balancetype,cardid,barcodeid,barcodetype," \
    "headercolor,starstatus,lastused,archive\n")
    
    # Обрабатываем каждую карточку
    for row in rows:
        try:
            user_id = row[0].split("/")[2]            
            loyalty_card_id = row[1]
            loyalty_card_json = json.loads(row[2])
        except json.JSONDecodeError:
            continue

        provider_ref = loyalty_card_json.get("input_provider_reference", {}).get("identifier")
        input_id = loyalty_card_json.get("input_id", "")
        input_barcode_format = loyalty_card_json.get("input_barcode_format", "")

        if not provider_ref:
            continue

        prov_split = provider_ref.split("/") 
        # Извлекаем ID карты
        provider_id = prov_split[-1]

        if provider_id in processed_providers:
            continue

        # Шаг 2: Получаем данные карты по id
        cursor.execute("""
            SELECT content FROM synced_resources
            WHERE id = ?
        """, (provider_id,))

        row_prov = cursor.fetchone()
        if not row_prov:
            print(f"Не найден контент для карты с id={provider_id}")
            continue

        try:
            provider_json = json.loads(row_prov[0])
        except json.JSONDecodeError:
            print(f"Ошибка разбора JSON для карты {provider_id}")
            continue

        name = provider_json.get("name")
        
        if not name:
            print(f"У карты {provider_id} нет поля 'name'")
            continue

        if (input_barcode_format == ""):
            input_barcode_format = provider_json.get("default_barcode_format", "QR_CODE")

        # Шаг 3: Получаем логотип карты
        logo_collection_like = f"%/loyalty-card-providers/{provider_id}/"
        cursor.execute("""
            SELECT content FROM synced_resources
            WHERE collection LIKE ? AND id = ?
        """, (logo_collection_like, "logo"))

        
        icon_filename = ""
        row_logo = cursor.fetchone()
        if not row_logo:
            print(f"Логотип не найден для карты {name}")
        else:
            logo_data = row_logo[0]
            icon_filename = f"{output_folder}/card_{provider_counter}_icon.png"
            # Сохраняем логотип            
            with open(icon_filename, "wb") as img_file:
                img_file.write(logo_data)

               
        # теперь картинки back, front
        back_collection_like = f"/users/{user_id}/loyalty-cards/{loyalty_card_id}/images/"
        cursor.execute("""
            SELECT id,content FROM synced_resources
            WHERE collection LIKE ?
        """, (back_collection_like,))

        print(f"SELECT id,content FROM synced_resources WHERE collection LIKE {back_collection_like}")
        
        images = cursor.fetchall()
        for img in images:            
            img_id = img[0]
            img_raw = img[1]
            # Сохраняем 
            filename = f"card_{provider_counter}_{img_id}.png"
            with open(f"{output_folder}/{filename}", "wb") as img_file:
                img_file.write(img_raw)

        # Записываем строку в текстовый файл        
        color = -9977996;
        if icon_filename != "":
            color = get_dominant_color_int(icon_filename)

        last_usage = 1750855369
        line = f"{provider_counter},{name},,,,0,,{input_id},,{input_barcode_format},{color},0,{last_usage},0\n"
        txt_file.write(line)

        print(f"Обработана карта: {name} → ID: {provider_counter}")

        # Увеличиваем счётчик
        provider_counter += 1
        processed_providers.add(provider_id)

    txt_file.write("\ncardId,groupId\n")

zip_files(output_folder,output_zip)
print("Готово!")

# Закрываем соединение
conn.close()

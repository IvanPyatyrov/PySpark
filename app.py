from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df, product_category_df):
    # Пары "Имя продукта - Имя категории"
    product_category_pairs = product_category_df.join(products_df, 
                                                      product_category_df.product_id == products_df.product_id, 
                                                      "inner") \
                                                 .join(categories_df, 
                                                       product_category_df.category_id == categories_df.category_id, 
                                                       "inner") \
                                                 .select(products_df.product_name, categories_df.category_name)

    # Продукты без категорий
    products_without_categories = products_df.join(product_category_df, 
                                                    products_df.product_id == product_category_df.product_id, 
                                                    "left_anti") \
                                              .select(products_df.product_name)

    return product_category_pairs, products_without_categories


# Пример использования
spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

# Пример датафреймов
products_data = [(1, 'Продукт A'), (2, 'Продукт B'), (3, 'Продукт C'), (4, 'Продукт D')]
categories_data = [(1, 'Категория X'), (2, 'Категория Y')]
product_category_data = [(1, 1), (1, 2), (2, 1)]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

# Вызов метода
pairs, without_categories = get_product_category_pairs(products_df, categories_df, product_category_df)

# Вывод результатов
pairs.show()
without_categories.show()

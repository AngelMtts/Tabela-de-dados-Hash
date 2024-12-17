# Tabela-de-dados-Hash
Uma tabela constando dados como nome e matrícula aleatória, onde foi criado uma matrícula hash para tornar cada matrícula única.

#começo do código importando a fake tabela, pandas e pyspark com regex

from faker import Faker
import pandas as pd
from pyspark.sql.functions import regexp_replace

# Inicializar Faker
fake = Faker('pt_BR')

# Gerar dados falsos
data = {
    'nome': [fake.name() for _ in range(10)],
    'matricula': [fake.random_number(digits=8, fix_len=True) for _ in range(10)]
}

# Criar um DataFrame pandas
df = pd.DataFrame(data)

# Converter pandas DataFrame para Spark DataFrame
spark_df = spark.createDataFrame(df)

# Aplicar regexp_replace no Spark DataFrame
spark_df = spark_df.withColumn(
    'nome',
    regexp_replace('nome', '(?<=\\w)[^\\s]', '*')
).withColumn(
    'matricula',
    regexp_replace('matricula', '(?<=\\w)[^\\s]', '*')
)


#Para mostrar no SQL

SELECT 
    nome, 
    matricula,
    sha2(CAST(matricula AS STRING), 256) AS matricula_hashed 
FROM 
    academia_inovacao.teste_de_programa.fake_data;


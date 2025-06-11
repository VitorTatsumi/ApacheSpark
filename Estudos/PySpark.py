import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

spark = SparkSession.builder.config("spark.ui.port", "4050").getOrCreate()

# Ler um arquivo ou DataFrame
# -Lê o arquivo CSV (Há diversos outros tipos de arquivos compatíveis)
# -O parâmetro header=True indica que a primeira linha do arquivo contém os nomes das colunas (Cabeçalho)
# -O parâmetro inferSchema=True indica que o Spark deve tentar identificar automaticamente o tipo de dados de cada coluna
df = spark.read.csv("Caminho do arquivo", header=True, inferSchema=True)

# Visualizar o dataFrame
df.show()

# Para visualizar o Top 5, é possível fazer utilizando o método .show(5)
# Semelhante ao "SELECT TOP 5" do SQL
df.show(5)

# Para contar o número de linhas do DataFrame
# Semelhante ao "SELECT COUNT(*)" do SQL
df.count()

# Atributo para visualizar as colunas do DataFrame
df.columns

# Para visualizar o tipo de dados de uma coluna específica
# Semelhante ao "SELECT nome_da_coluna" do SQL
df.select("nome_da_coluna")

# Para visualizar somente uma coluna especifica do DataFrame
df.select("nome_da_coluna").show()

# Para visualizar o Schema (tipo de dados de todas as colunas) do DataFrame
df.printSchema()

# Para visualizar dados distintos de uma coluna específica
# Semelhante ao "SELECT DISTINCT" do SQL
df.select("nome_da_coluna").distinct().show()

# Para filtrar dados com condições WHERE
# Semelhante ao "SELECT nome_da_coluna FROM tabela WHERE nome_da_coluna > 100" do SQL
df.where("nome_da_coluna > 100").show()

# Cria uma breve descrição estatística do DataFrame contendo:
# -Contagem de linhas (não nulos)
# -Média
# -Desvio padrão
# -Valor mínimo
# -Valor máximo
df.describe().show()

# Comando para ordenar os dados do DataFrame
# Semelhante ao "SELECT nome_da_coluna FROM tabela ORDER BY nome_da_coluna DESC" do SQL
# -Para ordenar de forma crescente, utilize ascending=True
# -Para ordenar de forma decrescente, utilize ascending=False
df.orderBy("nome_da_coluna", ascending=False).show()

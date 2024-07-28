from etl import executar_pipeline

# Parâmetros para a execução da pipeline
pasta_argumento = 'data'
formato_argumento = 'csv'  # ou 'parquet'

# Chamada da função para executar a pipeline
executar_pipeline(pasta=pasta_argumento, formato_saida=formato_argumento)


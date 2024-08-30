# sp500-analysis-TaTech
Proyecto de ETL y Análisis de Empresas del S&amp;P 500: Fase 6

Este proyecto es la práctica de todo lo visto en el Bootcamp de Análisis y Visualización de Datos. Comprende el desarrollo de un ETL para el precio de cierre en el primer trimestre del 2024 de las empresas del índice S&P Global Ratings, Insertar un dataframe de Python en una tabla SQL, un dashboard en power bi con el análisis estadístico del trimestre de la referencia y un análisis no supervisado de Machine Learning.

##Requisitos
Las herramientas necesarias son: 
  `Python`
  `Colab.google`
  `Sql server`
  `PowerBI`
Librerías: `pandas`, `sqlalchemy`, `pyodbc`, `scikit-learn`, `logging`, `os`


##Estructura del proyecto
- `data/`: Contiene los archivos CSV con los datos de las empresas y perfiles.
- `logs/`: Contiene los logs de los procesos ejecutados.
- `script/`: Contiene el script ETL.py para el ETL del proyecto.
- `dashboard/`: Contiene el archivo .pbix de Power BI.
- `notebook/`: Contiene el archivo .ipynb de clustering o algoritmo de agrupamiento.
- `README.md`: Documento explicativo del proyecto.

## Instrucciones de Instalación y Uso
1. Clona este repositorio:
 git clone https://github.com/tuusuario/sp500-analysis.git
 cd sp500-analysis
2. Instala las dependencias necesarias:
pip install -r requirements.txt
3. Configura la conexión a SQL Server en el script ETL.py
4. Ejecuta el script para realizar el ETL e Insertar un dataframe de Python en una tabla SQL
5. Ejecuta el dashboard para la visualización
6. Ejecuta el Notebook ETL y Clusterización de Empresas del S&P 500

##Fases del Proyecto 
Fase 1 y 3: ETL y Almacenamiento en SQL Server
• Obtención de datos de empresas del S&P 500 desde Wikipedia. 
• Descarga de los precios de cotización del último año. 
• Carga de los datos limpios en una base de datos SQL Server. 
Fase 4: Dashboard en Power BI 
• Creación de un dashboard interactivo con KPIs, tooltips y bookmarks. 
Fase 5: Clusterización de las Acciones 
• Agrupamiento de las acciones en clusters según indicadores de volatilidad. 
Fase 6: Publicación en GitHub 
• Subida del proyecto al repositorio de GitHub y documentación en este 
archivo README.md.

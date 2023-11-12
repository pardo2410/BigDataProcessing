
# Práctica Big Data Processing
## Descubriendo el manejo de Big Data con Spark en conjunto con Scala y Pyspark: Lecciones aprendidas.

Las grandes cantidades de información y datos disponibles hoy en el mundo digital, hacen que la capacidad para organizarlos y analizarlos a fin de extraer información de valor que permita la toma de decisiones estratégicas, sea uno de los objetivos fundamentales de la ciencia de datos. Para lograr este cometido, se pueden utilizar diferentes herramientas de programación; no obstante, el aprendizaje de Spark junto con PySpark y Scala me ha proporcionado valiosos conocimientos que han ampliado significativamente mi comprensión y dominio de grandes volúmenes de datos. El propósito de este artículo es compartir tres de los principales aprendizajes adquiridos con el uso de estas herramientas:

# 1.	Spark 
 
Es un framework orientado al procesamiento de datos masivos con gran potencial para distribuir, gestionar y analizar datos de forma unificada y rápida. Algunas de las características más distintivas de este entorno son: su programación funcional, su interfaz expresiva y concisa, su orientación a objetos, su versatilidad para manejar la gran mayoría de gestores de datos – como MySQL, Kakfka, Elastic, Hadoop, entre otros – y su capacidad de integración con diferentes API´s como Java, Scala, Python y R, permitiendo la integración de herramientas de alto nivel que facilitan la manipulación y procesamiento de datos.  

Comprender los diferentes componentes que conforman la arquitectura de Apache Spark me permitió descubrir el potencial del motor central de este sistema. El ecosistema de Apache Spark está conformado por: Spark Core, Spark SQL, Spark, Mlib, Spark Graph X y Spark Streaming. Uno de los núcleos de Spark que encontré más útil para trabajar el procesamiento de datos fue Spark SQL, el cual facilita gestionar datos estructurados y semiestructurados, así como hace consultas SQL a través de la implementación de Dataframes, las cuales son tablas racionales que pueden construirse a partir de ficheros estructurados.   

# 2.	Scala 

Es un lenguaje de programación funcional y orientado a objetos, que proporciona una interfaz concisa y expresiva para el procesamiento distribuido de datos, y que se integra de forma simbiótica con Spark, de tal manera que el lenguaje sea más legible. Algunas de las cualidades más destacables de Scala son: su capacidad para crecer y escalar con la demanda, así como su interoperabilidad con Java. Todo lo anterior, facilita la interacción entre bases de datos distribuidas y su procesamiento paralelo, ayudando al procesamiento de datos.    

La combinación de Spark y Scala potencia significativamente el procesamiento de datos como consecuencia de las estructuras de RDD (Resilient Distributed Datasets), que facilitan la realización de tareas de Transformación y Acciones sobre los conjuntos de datos. Estas Transformaciones posibilitan la creación de un nuevo RDD a partir de uno ya existente, permitiendo llevar a cabo operaciones específicas, como map o filter; mientras que, las Acciones representan procesos que devuelven un resultado al programa definido por el usuario, como count, max o collect, entre otros. Scala agiliza la ejecución de estas instrucciones de manera eficiente y funcional, optimizando así la manipulación de datos en entornos distribuidos.

# 3.	PySpark

PySpark es una interfaz que facilita la ejecución de código Python dentro del entorno de Apache Spark, ofreciendo a los desarrolladores Python una entrada a las herramientas especializadas en el procesamiento de grandes volúmenes de datos integradas en Spark. Siguiendo la misma línea de Scala, PySpark permite la manipulación de RDDs (Resilient Distributed Datasets) y DataFrames. Los RDDs son estructuras de datos inmutables distribuidas; mientras que, los DataFrames proporcionan una interfaz optimizada y de fácil uso para el análisis de datos estructurados. A partir del uso de estas dos estructuras PySpark se convierte en una herramienta versátil y valiosa para el análisis y procesamiento eficiente de grandes conjuntos de datos desde el entorno de Python.
 
La práctica llevada a cabo en el módulo de Big Data Processing ha sido esencial para consolidar mis habilidades en PySpark, permitiéndome realizar procesos de preparación, depuración, procesamiento y consultas en las bases de datos. La puesta en práctica de los conocimientos adquiridos la hice a través de un proyecto de implementación de Pyspark, en el que se procesaron dos conjuntos de datos que contienen información anual por países sobre indicadores de felicidad, GDP, corrupción, expectativa de vida, entre otros. Una vez procesados los datos, se hizo un análisis de los mismos para la respuesta de preguntas concretas. Este proyecto se puede consultar a través del siguiente enlace de GitHub:
![Link medium:](https://medium.com/@juandavidpardo/descubriendo-el-manejo-de-big-data-con-spark-en-conjunto-con-scala-y-pyspark-lecciones-aprendidas-c4171ff2df91)
#  
# Link GitHub: https://github.com/pardo2410/BigDataProcessing.git


En conclusión, a lo largo del módulo de Big Data Processing tuve un acercamiento a las herramientas de programación Spark, Scala y Pyspark, el cual me proporcionó habilidades técnicas en el manejo de datos a gran escala, me ofreció insights valiosos sobre la distribución de datos, la optimización del código y la escalabilidad de los mismos. Estos aprendizajes son esenciales en un mundo donde la cantidad de datos continúa creciendo exponencialmente, y la capacidad para procesarlos eficientemente se ha vuelto crucial para el éxito en el ámbito de la ciencia de datos y el análisis de Big Data.






fantasma
estado de construcción descargas lanzamiento

Migración de esquemas en línea de GitHub para MySQL
gh-ostes una solución de migración de esquemas en línea sin disparadores para MySQL. Es comprobable y proporciona pausabilidad, control dinámico / reconfiguración, auditoría y muchas ventajas operativas.

gh-ost produce una carga de trabajo ligera en el maestro durante la migración, desacoplada de la carga de trabajo existente en la tabla migrada.

Ha sido diseñado en base a años de experiencia con soluciones existentes y cambia el paradigma de las migraciones de tablas.

¿Cómo?
Todas las herramientas de cambio de esquema en línea existentes operan de manera similar: crean una tabla fantasma a semejanza de su tabla original, migran esa tabla mientras está vacía, copian lenta e incrementalmente los datos de su tabla original a la tabla fantasma , mientras propagan los cambios en curso (ninguna INSERT, DELETE, UPDATEaplicada a su mesa) al fantasma mesa. Finalmente, en el momento adecuado, reemplazan su mesa original con la mesa fantasma .

gh-ostusa el mismo patrón. Sin embargo, se diferencia de todas las herramientas existentes por no utilizar activadores. Hemos reconocido que los factores desencadenantes son la fuente de muchas limitaciones y riesgos .

En su lugar, gh-ost utiliza el flujo de registro binario para capturar los cambios de la tabla y los aplica de forma asincrónica a la tabla fantasma . gh-ostasume algunas tareas que otras herramientas dejan para que las realice la base de datos. Como resultado, gh-osttiene un mayor control sobre el proceso de migración; verdaderamente puede suspenderlo; realmente puede desacoplar la carga de escritura de la migración de la carga de trabajo del maestro.

Además, ofrece muchas ventajas operativas que lo hacen más seguro, confiable y divertido de usar.

flujo general de gh-ost

Destacar
Desarrolle su confianza gh-ostprobándolo en réplicas. gh-ostemitirá el mismo flujo que tendría en el maestro, para migrar una tabla en una réplica, sin reemplazar realmente la tabla original, dejando la réplica con dos tablas que luego puede comparar y asegurarse de que la herramienta funciona correctamente. Así es como probamos continuamente gh-osten producción.
Pausa verdadera: cuando gh-ost acelera , realmente deja de escribir en el maestro: no hay copias de fila ni procesamiento de eventos en curso. Al estrangular, devuelve su maestro a su carga de trabajo original
Control dinámico: puede reconfigurar de forma interactivagh-ost , incluso cuando la migración aún se está ejecutando. Puede iniciar la aceleración a la fuerza.
Auditoría: puede consultar el gh-ostestado. gh-ostescucha en socket Unix o TCP.
Control sobre la fase de corte: gh-ostse le puede indicar que posponga lo que probablemente sea el paso más crítico: el intercambio de mesas, hasta el momento en que esté cómodamente disponible. No hay necesidad de preocuparse de que ETA esté fuera del horario de oficina.
Los ganchos externos se pueden acoplar gh-osta su entorno particular.
Consulte los documentos para obtener más información. No, de verdad, lee los documentos .

Uso
La hoja de trucos lo tiene todo. Puede que le interese invocar gh-osten varios modos:

una migración noop (simplemente probando que la migración es válida y está lista para comenzar )
una migración real, utilizando una réplica (la migración se ejecuta en el maestro; gh-ostaverigua las identidades de los servidores involucrados. Modo obligatorio si su maestro usa la replicación basada en declaraciones)
una migración real, se ejecuta directamente en el maestro (pero gh-ostprefiere el primero)
una migración real en una réplica (maestra sin tocar)
una migración de prueba en una réplica, la forma de generar confianza en gh-ostla operación de '.
Nuestros consejos:

Probando sobre todo , pruébelo las --test-on-replicaprimeras veces. Mejor aún, hazlo continuo. Tenemos varias réplicas en las que iteramos toda nuestra flota de tablas de producción, migrándolas una por una, sumando los resultados y verificando que la migración sea buena.
Para cada migración maestra, primero emita un noop
Luego emita el verdadero vía --execute.
Mas consejos:

Úselo --exact-rowcountpara una indicación precisa del progreso
Úselo --postpone-cut-over-flag-filepara obtener control sobre el tiempo de corte
Familiarízate con los comandos interactivos
Ver también:

requisitos y limitaciones
preguntas comunes
¿y si?
la letra pequeña
Preguntas de la comunidad
Utilizando gh-osten AWS RDS
Usar gh-osten Azure Database for MySQL
¿Lo que hay en un nombre?
Originalmente, esto se llamaba gh-osc: GitHub Online Schema Change, como el cambio de esquema en línea de Facebook y pt-online-schema-change .

Pero luego ocurrió una rara mutación genética y se ctransformó en t. Y eso nos envió por el camino de tratar de encontrar un nuevo acrónimo. gh-ost(pronunciar: Ghost ), significa Transmogrificador / Traductor / Transformador / Transfigurador de esquemas en línea de GitHub

Licencia
gh-osttiene licencia de MIT

gh-ostutiliza bibliotecas de terceros, cada una con su propia licencia. Estos se encuentran aquí .

Comunidad
gh-ostse lanza en un estado estable, pero con kilometraje por recorrer. Estamos abiertos a solicitudes de extracción . Primero discuta sus intenciones a través de Problemas .

Desarrollamos gh-osten GitHub y para la comunidad. Es posible que tengamos prioridades diferentes a las de otros. De vez en cuando podemos sugerir una contribución que no está en nuestra hoja de ruta inmediata pero que puede resultar atractiva para otros.

Consulte Codificación de gh-ost para obtener una guía para comenzar a desarrollar con gh-ost.

Descargar / binarios / fuente
gh-ost ahora es GA y estable.

gh-ost está disponible en formato binario para Linux y Mac OS / X

Descargue la última versión aquí

gh-ostes un proyecto Go; está construido con Go 1.14y superior. Para construir por su cuenta, use:

script / build : este es el mismo script de compilación utilizado por CI, por lo tanto, la autoridad; el artefacto es ./bin/gh-ostbinario.
build.sh para construir tar.gzartefactos en/tmp/gh-ost
En términos generales, la masterrama es estable, pero solo las versiones deben usarse en producción.

Autores
gh-ost está diseñado, creado, revisado y probado por el equipo de infraestructura de la base de datos en GitHub:

@jonahberquist
@ggunson
@tomkrouper
@ shlomi-noach
@jessbreckenridge
@gtowey
@timvaillancourt
Lanzamientos 45
Versión de GA v1.1.0
Últi

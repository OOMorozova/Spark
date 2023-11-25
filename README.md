# [Spark](https://stepik.org/course/115252/syllabus)

[2.1.1 DFs Основы](src/main/scala/DF.scala)
>Создание датафрейма.

[2.1.2 Schema](src/main/scala/Schema.scala)
>Код, выполняющий загрузку данных из файла.\
>Данные в колонках is_delivering_now и has_online_delivery должны в конечном итоге иметь тип IntegerType.\
>В коде предусмотрите возможность наглядной демонстрации выполнения данного условия.

[2.2.1 Read & Write DF](src/main/scala/ReadWrite.scala)
>Создать схему и считать файл.\
>Вывести схему.\
>Сохранить файл в формате parquet.

[2.3.1 Select columns](src/main/scala/Columns.scala)
>Из DF должны быть выбраны  колонки: Hour, TEMPERATURE, HUMIDITY, WIND_SPEED.\
>На экран должны быть выведены первые три строчки данных из выбранных колонок.

[2.3.2 New column, when-otherwise, distinct](src/main/scala/Uniq.scala)
>Создать колонку is_workday, состоящую из значений 0 и 1:\
>[0 - если значением колонки HOLIDAY является Holiday и значением колонки FUNCTIONING_DAY является No].\
>Выведите на экран строчки, которые включают в себя только уникальные значения из колонок  
>"HOLIDAY", "FUNCTIONING_DAY", "is_workday"

[2.3.3 GroupBy, Agg](src/main/scala/MinMax.scala)
>Рассчитывается минимальное и максимальное значение температуры TEMPERATURE для каждой даты Date.\
>Результат расчетов записывается в колонки min_temp, max_temp и выводится на экран.
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

[2.5.1 Practice 1](src/main/scala/MallCustomers.scala)
>Исправить ошибку в данных (колонка Age). Возраст клиентов был записан неправильно. Вышло так, что все оказались на 2 года младше, чем они есть на самом деле.\
>Разделить клиентов на группы и подсчитать среднее значение Annual Income (k$) для каждой группы:
> * Рассматриваются лица, реальный возраст которых от 30 до 35 лет (включительно).
> * Клиенты делятся на группы в зависимости от пола и возраста.
> * Полученное среднее значение должно быть округлено до 1 знака после запятой.
>
> Записи в датафрейме должны быть отсортированы по полу и возрасту (в порядке возрастания).\
>Создать колонку gender_code, в которую поместить значение 1 (для клиентов мужского пола ) или 0 (для клиентов женского пола).

[2.5.2 Practice 2](src/main/scala/Subtitles.scala)
> Для каждого сезона найти топ-20 самых часто используемых слов (Word и word - это одно и то же слово).\
> Результаты расчетов объединить в общий датафрейм: 
> * w_s1 - колонка, содержащая слова из первого сезона;
> * w_s2 - колонка, содержащая слова из второго сезона;
> * в cnt_s1 и cnt_s2  - данные о том, сколько раз слово встретилось в субтитрах для первого (cnt_s1) и второго сезона (cnt_s2);
> * колонка id  содержит числа от 0 до 19 включительно.


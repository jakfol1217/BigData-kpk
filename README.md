# Projekt z przedmiotu Big Data- zespół KPK
* **Autorzy: Paulina Jaszczuk, Kacper Grzymkowski, Jakub Fołtyn**  
Niniejsze repozytorium obejmuje projekt z przedmiotu Big Data, realizowanego w ramach studiów inżynierskich na wydziale Matematyki i Nauk Informacyjnych Politechniki Warszawskiej.  
Projekt skupia się na przetwarzaniu i analizie danych dotyczących autobusów warszawskiego transportu miejskiego (udostępnionych przez ZTM), pozyskiwanych ze strony [Dane po warszawsku](https://api.um.warszawa.pl/). Ponadto analizy wzbogacane są danymi dotyczącymi warunków pogodowych ze strony [Meteostat](https://meteostat.net/en/). Przetwarzanie oraz składowanie danych wykonywane jest przy użyciu narzędzi i oprogramowania związanego z szeroko rozumianym pojęciem dziedziny Big Data.
## Struktura folderów i plików:
* `nifi\` - folder ten zawiera szablony przetwarzania w Apache Nifi, jak i wszelkie skrypty "pomocnicze", wspomagające owe przetwarzanie.
* `enrichment\` - folder ten zawiera skrypty PySpark związane z ubogacaniem danych dotyczących autobusów.
* `spark_analysis\` - folder zawierający skrypty z dalszymi analizami w PySpark.
* `hbase\` - folder zawierający skrypty tworzące bazy danych HBase, jak i również umożliwiające dostęp do HBase z poziomu jupyter notebook.
* `visualization\` - folder zawierający notatnik Jupyter notebook z wizualizacjami.

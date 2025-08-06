from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, instr, when, regexp_replace, explode, lit
from pyspark.sql.functions import replace as spark_replace

import os

from config import SPARK_ENGINE_KRSAPI_URL, SPARK_SOURCE_KAFKA_URL, SPARK_ENGINE_KRSAPI_CHECKPOINTS
from spark_etl.schemas.debezium_envelope_schema import schema as debezium_schema
from spark_etl.schemas.krs_api_json_schema import schema as krs_api_schema
from spark_etl.functions.write_to_psq_batch import write_to_postgres_dynamic


def run_krs_api_stream():
    # Starting spark session 
    # With packages for psql, kafka and avro
    spark = SparkSession.builder \
        .appName("KRSAPI_Spark_App") \
        .master(SPARK_ENGINE_KRSAPI_URL) \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                "org.apache.spark:spark-avro_2.13:4.0.0,"
                "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    # Reading from kafka stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", SPARK_SOURCE_KAFKA_URL) \
        .option("subscribe", "postgres.public.raw_krs_api_full_extract") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parsing input send from debezium (trimming metadata)
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .withColumn("data", from_json(col("json_str"), debezium_schema)) \
        .withColumn("id", col("data.after.id")) \
        .withColumn("numer_krs", col("data.after.krs_number")) \
        .withColumn("raw_data", col("data.after.raw_data"))

    # Changing key values for companies based abroad (their keys have 'Zagranicznego' in some key names)
    parsed_df = parsed_df \
        .withColumn("czy_firma_zagraniczna", when(instr(col("raw_data"), "danePodmiotuZagranicznego")>0, True).otherwise(False)) \
        .withColumn("raw_data", regexp_replace(
            col("raw_data"),
            r'"siedzibaIAdresPodmiotuZagranicznego"\s*:',
            r'"siedzibaIAdres":'
        )) \
        .withColumn("raw_data", regexp_replace(
            col("raw_data"), 
            r'"([a-zA-Z]+)Zagranicznego"\s*:',
            r'"$1":')) \
        .withColumn("parsed", from_json(col("raw_data"), krs_api_schema))


    # Extracting data and loading it into related tables
    df_nazwa = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.danePodmiotu.nazwa").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.nazwa").alias("nazwa_firmy"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )


    df_forma_prawna = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.danePodmiotu.formaPrawna").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.formaPrawna").alias("forma_prawna"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_identyfikatory = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.danePodmiotu.identyfikatory").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.identyfikatory.nip").alias("numer_nip"),
        col("entry.identyfikatory.regon").alias("numer_regon"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_czy_posiada_status_opp = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.danePodmiotu.czyPosiadaStatusOPP").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.czyPosiadaStatusOPP").alias("czy_posiada_status_opp"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_czy_prowadzi_dzialalnosc_z_innymi_podmiotami = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.danePodmiotu.czyProwadziDzialalnoscZInnymiPodmiotami").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.czyProwadziDzialalnoscZInnymiPodmiotami").alias("czy_prowadzi_dzialalnosc_z_innymi_podmiotami"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_adres = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.siedzibaIAdres.adres").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.kraj").alias("kraj"),
        col("entry.ulica").alias("ulica"),
        col("entry.nrDomu").alias("nr_domu"),
        col("entry.nrLokalu").alias("nr_lokalu"),
        col("entry.poczta").alias("poczta"),
        col("entry.kodPocztowy").alias("kod_pocztowy"),
        col("entry.miejscowosc").alias("miejscowosc"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_siedziba = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.siedzibaIAdres.siedziba").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.kraj").alias("kraj"),
        col("entry.gmina").alias("gmina"),
        col("entry.powiat").alias("powiat"),
        col("entry.miejscowosc").alias("miejscowosc"),
        col("entry.wojewodztwo").alias("wojewodztwo"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )


    df_adres_strony_internetowej = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.siedzibaIAdres.adresStronyInternetowej").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.adresStronyInternetowej").alias("adres_strony_internetowej"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_adres_poczty_elektronicznej = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.siedzibaIAdres.adresPocztyElektronicznej").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.adresPocztyElektronicznej").alias("adres_poczty_elektronicznej"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_adres_do_doreczen_elektronicznych_wpisany_do_bae = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.siedzibaIAdres.adresDoDoreczenElektronicznychWpisanyDoBAE").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.adresDoDoreczenElektronicznychWpisanyDoBAE") \
            .alias("adres_do_doreczen_elektronicznych_wpisany_do_bae"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_siedziba_i_adres_zakladu_glownego = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.siedzibaIAdres.siedzibaIAdresZakladuGlownego").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.kraj").alias("kraj"),
        col("entry.ulica").alias("ulica"),
        col("entry.nrDomu").alias("nr_domu"),
        col("entry.nrLokalu").alias("nr_lokalu"),
        col("entry.poczta").alias("poczta"),
        col("entry.kodPocztowy").alias("kod_pocztowy"),
        col("entry.miejscowosc").alias("miejscowosc"),
        col("entry.wojewodztwo").alias("wojewodztwo"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_informacja_o_czasie_na_jaki_zostal_utworzony_podmiot = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.pozostaleInformacje.informacjaOCzasieNaJakiZostalUtworzonyPodmiot").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.czasNaJakiUtworzonyZostalPodmiot").alias("czas_na_jaki_zostal_utworzony_podmiot"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_informacja_o_zawarciu_zmianie_umowy_statutu = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.umowaStatut.informacjaOZawarciuZmianieUmowyStatutu").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("entry.pozycja").alias("pozycja")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("pozycja.zawarcieZmianaUmowyStatutu").alias("zawarcie_zmiana_umowy_statutu"),
        col("pozycja.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("pozycja.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_wysokosc_kapitalu_zakladowego = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.kapital.wysokoscKapitaluZakladowego").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.waluta").alias("waluta"),
        col("entry.wartosc").alias("wartosc"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_laczna_liczba_akcji_udzialow = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.kapital.lacznaLiczbaAkcjiUdzialow").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.lacznaLiczbaAkcjiUdzialow").alias("laczna_liczba_akcji_udzialow"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_wartosc_jednej_akcji = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.kapital.wartoscJednejAkcji").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.waluta").alias("waluta"),
        col("entry.wartosc").alias("wartosc"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )

    df_czesc_kapitalu_wplaconego_pokrytego = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.kapital.czescKapitaluWplaconegoPokrytego").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.waluta").alias("waluta"),
        col("entry.wartosc").alias("wartosc"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano"),
        col("entry.nrWpisuWykr").cast("int").alias("nr_wpisu_usunieto")
    )


    df_emisje_akcji_nazwa_serii_akcji = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.emisjeAkcji").alias("emisja")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("emisja.nazwaSeriiAkcji").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.nazwaSeriiAkcji").alias("nazwa_serii"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano")
    )

    df_emisje_akcji_liczba_w_serii = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.emisjeAkcji").alias("emisja")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("emisja.liczbaAkcjiWSerii").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.liczbaAkcjiWSerii").alias("liczba_akcji"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano")
    )


    df_wpisy = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.naglowekP.wpis").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.opis").alias("opis"),
        col("entry.dataWpisu").alias("data_wpisu"),
        col("entry.numerWpisu").alias("numer_wpisu"),
        col("entry.oznaczenieSaduDokonujacegoWpisu").alias("oznaczenie_sady_dokonujacego_wpisu"),
        col("entry.sygnaturaAktSprawyDotyczacejWpisu").alias("sygnatura_akt_sprawy_dotyczacej_wpisu")
    )

    df_emisje_akcji_uprzywilejowanie = parsed_df.select(
        col("id").alias("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("parsed.odpis.dane.dzial1.emisjeAkcji").alias("emisja")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        explode("emisja.czyAkcjeUprzywilejowaneLiczbaAkcjiRodzajUprzywilejowania").alias("entry")
    ).select(
        col("fk_raw_extract"),
        col("numer_krs"),
        col("czy_firma_zagraniczna"),
        col("entry.czyAkcjeUprzywilejowaneLiczbaAkcjiRodzajUprzywilejowania").alias("uprzywilejowanie_info"),
        col("entry.nrWpisuWprow").cast("int").alias("nr_wpisu_dodano")
    )



    # List of all tables that were populated
    tables = {
        "company_names": df_nazwa,
        "legal_forms": df_forma_prawna,
        "identifiers": df_identyfikatory,
        "status_opp": df_czy_posiada_status_opp,
        "cooperation": df_czy_prowadzi_dzialalnosc_z_innymi_podmiotami,
        "addresses": df_adres,
        "headquarters": df_siedziba,
        "websites": df_adres_strony_internetowej,
        "emails": df_adres_poczty_elektronicznej,
        "elec_delivery_addr": df_adres_do_doreczen_elektronicznych_wpisany_do_bae,
        "main_branch": df_siedziba_i_adres_zakladu_glownego,
        "duration_info": df_informacja_o_czasie_na_jaki_zostal_utworzony_podmiot,
        "statute_changes": df_informacja_o_zawarciu_zmianie_umowy_statutu,
        "share_capital": df_wysokosc_kapitalu_zakladowego,
        "total_shares": df_laczna_liczba_akcji_udzialow,
        "share_value": df_wartosc_jednej_akcji,
        "paid_up_capital": df_czesc_kapitalu_wplaconego_pokrytego,
        "share_series": df_emisje_akcji_nazwa_serii_akcji,
        "shares_in_series": df_emisje_akcji_liczba_w_serii,
        "entries": df_wpisy,
        "share_privileges": df_emisje_akcji_uprzywilejowanie
    }

    # Table queries being loaded into DB
    queries = [] 

    # For each table, load data from each batch
    for table_name, df in tables.items():
        query = df.writeStream \
            .foreachBatch(write_to_postgres_dynamic(df, table_name)) \
            .outputMode("append") \
            .option("checkpointLocation", f"{SPARK_ENGINE_KRSAPI_CHECKPOINTS}_{table_name}") \
            .start()
        queries.append(query)

    for q in queries:
        q.awaitTermination()

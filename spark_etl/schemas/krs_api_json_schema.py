from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType


schema = StructType([
    StructField("odpis", StructType([
        StructField("dane", StructType([
            StructField("dzial1", StructType([
                StructField("danePodmiotu", StructType([
                    StructField("nazwa", ArrayType(StructType([
                        StructField("nazwa", StringType(), True),
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True)
                    ]))),
                    StructField("formaPrawna", ArrayType(StructType([
                        StructField("formaPrawna", StringType(), True),
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True)
                    ]))),
                    StructField("identyfikatory", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("identyfikatory", StructType([
                            StructField("nip", StringType(), True),
                            StructField("regon", StringType(), True)
                        ]))
                    ]))),
                    StructField("czyPosiadaStatusOPP", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("czyPosiadaStatusOPP", BooleanType(), True)
                    ]))),
                    StructField("czyProwadziDzialalnoscZInnymiPodmiotami", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("czyProwadziDzialalnoscZInnymiPodmiotami", BooleanType(), True)
                    ])))
                ])),
                StructField("siedzibaIAdres", StructType([
                    StructField("adres", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("kraj", StringType(), True),
                        StructField("ulica", StringType(), True),
                        StructField("nrDomu", StringType(), True),
                        StructField("nrLokalu", StringType(), True),
                        StructField("poczta", StringType(), True),
                        StructField("kodPocztowy", StringType(), True),
                        StructField("miejscowosc", StringType(), True),
                    ]))),
                    StructField("siedziba", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("kraj", StringType(), True),
                        StructField("gmina", StringType(), True),
                        StructField("powiat", StringType(), True),
                        StructField("miejscowosc", StringType(), True),
                        StructField("wojewodztwo", StringType(), True)
                        
                    ]))),
                    StructField("adresStronyInternetowej", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("adresStronyInternetowej", StringType(), True)
                    ]))),
                    StructField("adresPocztyElektronicznej", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("adresPocztyElektronicznej", StringType(), True)
                    ]))),
                    StructField("adresDoDoreczenElektronicznychWpisanyDoBAE", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("adresDoDoreczenElektronicznychWpisanyDoBAE", StringType(), True)
                    ]))),
                    StructField("siedzibaIAdresZakladuGlownego", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("kraj", StringType(), True),
                        StructField("ulica", StringType(), True),
                        StructField("nrDomu", StringType(), True),
                        StructField("nrLokalu", StringType(), True),
                        StructField("poczta", StringType(), True),
                        StructField("kodPocztowy", StringType(), True),
                        StructField("miejscowosc", StringType(), True),
                        StructField("wojewodztwo", StringType(), True),
                    ]))),
                ])),
                StructField("pozostaleInformacje", StructType([
                    StructField("informacjaOCzasieNaJakiZostalUtworzonyPodmiot", ArrayType(StructType([
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                        StructField("czasNaJakiUtworzonyZostalPodmiot", StringType(), True)
                    ])))
                ])),
                StructField("umowaStatut", StructType([
                    StructField("informacjaOZawarciuZmianieUmowyStatutu", ArrayType(StructType([
                        StructField("pozycja", ArrayType(StructType([
                            StructField("nrWpisuWykr", StringType(), True),
                            StructField("nrWpisuWprow", StringType(), True),
                            StructField("zawarcieZmianaUmowyStatutu", StringType(), True)
                        ])))
                    ])))
                ])),
                StructField("kapital", StructType([
                    StructField("wartoscJednejAkcji", ArrayType(StructType([
                        StructField("waluta", StringType(), True),
                        StructField("wartosc", StringType(), True),
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ]))),
                    StructField("lacznaLiczbaAkcjiUdzialow", ArrayType(StructType([
                        StructField("lacznaLiczbaAkcjiUdzialow", StringType(), True),
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ]))),
                    StructField("wysokoscKapitaluZakladowego", ArrayType(StructType([
                        StructField("waluta", StringType(), True),
                        StructField("wartosc", StringType(), True),
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ]))),
                    StructField("czescKapitaluWplaconegoPokrytego", ArrayType(StructType([
                        StructField("waluta", StringType(), True),
                        StructField("wartosc", StringType(), True),
                        StructField("nrWpisuWykr", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ])))
                ])),
                StructField("emisjeAkcji", ArrayType(StructType([
                    StructField("nazwaSeriiAkcji", ArrayType(StructType([
                        StructField("nazwaSeriiAkcji", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ]))),
                    StructField("liczbaAkcjiWSerii", ArrayType(StructType([
                        StructField("liczbaAkcjiWSerii", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ]))),
                    StructField("czyAkcjeUprzywilejowaneLiczbaAkcjiRodzajUprzywilejowania", ArrayType(StructType([
                        StructField("czyAkcjeUprzywilejowaneLiczbaAkcjiRodzajUprzywilejowania", StringType(), True),
                        StructField("nrWpisuWprow", StringType(), True),
                    ])))
                ])))
            ]))
        ])),
        StructField("naglowekP", StructType([
            StructField("wpis", ArrayType(StructType([
                StructField("opis", StringType(), True),
                StructField("dataWpisu", StringType(), True),
                StructField("numerWpisu", StringType(), True),
                StructField("oznaczenieSaduDokonujacegoWpisu", StringType(), True),
                StructField("sygnaturaAktSprawyDotyczacejWpisu", StringType(), True)
            ])))
        ]))
    ]))
])
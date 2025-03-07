# mappings.py

# Tournament name mapping
tournament_name_mapping = {
    "French Open": "Roland Garros",
    "BNP Paribas Open": "Indian Wells Masters",
    "Western & Southern Financial Group Masters": "Cincinnati Masters",
    "Mutua Madrid Open": "Madrid Masters",
    "Geneva Open": "Geneva",
    "Lyon Open": "Lyon",
    "Sony Ericsson Open": "Miami Masters",
    "Heineken Open": "Auckland",
    "Masters Cup": "Tour Finals",
    "Winston-Salem Open at Wake Forest University": "Winston-Salem",
    "AEGON Championships": "Queen's Club",
    "AnyTech365 Andalucia Open": "Marbella"
}

# Player name mapping
atp_player_name_mapping = {
    "Dutra Silva R.": "Rogerio Dutra Silva",
    "Mcdonald M.": "Mackenzie McDonald",
    "Albot R.": "Radu Albot",
    "Auger-Aliassime F.": "Felix Auger Aliassime",
    "Bagnis F.": "Facundo Bagnis",
    "Baez S.": "Sebastian Baez",
    "Djere L.": "Laslo Djere",
    "Etcheverry T.": "Tomas Martin Etcheverry",
    "Carballes Baena R.": "Roberto Carballes Baena",
    "Menendez-Maceiras A.": "Adrian Menendez Maceiras",
    "Van De Zandschulp B.": "Botic Van De Zandschulp",
    "Ramos-Vinolas A.": "Albert Ramos",
    "Carreno-Busta P.": "Pablo Carreno Busta",
    "Carreno Busta P.": "Pablo Carreno Busta",
    "Bautista Agut R.": "Roberto Bautista Agut",
    "O Connell C.": "Christopher Oconnell",
    "De Minaur A.": "Alex De Minaur",
    "Zapata Miralles B.": "Bernabe Zapata Miralles",
    "Davidovich Fokina A.": "Alejandro Davidovich Fokina",
    "Del Potro J.M.": "Juan Martin del Potro",
    "Gomez A.": "Alejandro Gomez",
    "Alejandro Gomez Gb42": "Alejandro Gomez",
    "Roca Batalla O.": "Oriol Roca Batalla",
    "Lopez San Martin A.": "Alvaro Lopez San Martin",
    "Garcia-Lopez G.": "Guillermo Garcia Lopez",
    "Munoz De La Nava D.": "Daniel Munoz de la Nava",
    "Vilella Martinez M.": "Mario Vilella Martinez",
    "Llamas Ruiz P.": "Pablo Llamas Ruiz",
    "De Schepper K.": "Kenny De Schepper",
    "De Bakker T.": "Thiemo De Bakker",
    "Seyboth Wild T.": "Thiago Seyboth Wild",
    "Kuznetsov An.": "Andrey Kuznetsov",
    "Nava E.": "Emilio Nava",
    "Haider-Maurer A.": "Andreas Haider Maurer",
    "Basilashvili N.": "Nikoloz Basilashvili"
}

# List of players who should not have their last name extracted
atp_players_no_last_name_extraction = set(atp_player_name_mapping.values())


# MAGIC %python
# MAGIC colour_modification_enum = {
# MAGIC     "BLACK": "BK",
# MAGIC     "SCHWARZ": "BK",
# MAGIC     "SW": "BK",
# MAGIC     "BRAUN": "BN",
# MAGIC     "BROWN": "BN",
# MAGIC     "BR": "BN",
# MAGIC     "BLAU": "BU",
# MAGIC     "BLUE": "BU",
# MAGIC     "BL": "BU",
# MAGIC     "GRÜN": "GN",
# MAGIC     "GREEN": "GN",
# MAGIC     "GRU": "GN",
# MAGIC     "GN": "GN",
# MAGIC     "GRAU": "GY",
# MAGIC     "GREY": "GY",
# MAGIC     "GR": "GY",
# MAGIC     "ORANGE": "OG",
# MAGIC     "ORG": "OG",
# MAGIC     "OR": "OG",
# MAGIC     "ROSA": "PK",
# MAGIC     "PINK": "PK",
# MAGIC     "RO": "PK",
# MAGIC     "RS": "PK",
# MAGIC     "ROT": "RD",
# MAGIC     "RED": "RD",
# MAGIC     "RT": "RD",
# MAGIC     "TK": "TQ",
# MAGIC     "VIO": "VT",
# MAGIC     "VIOL": "VT",
# MAGIC     "VIOLET": "VT",
# MAGIC     "VIOLETT": "VT",
# MAGIC     "VI": "VT",
# MAGIC     "WEIß": "WH",
# MAGIC     "WHITE": "WH",
# MAGIC     "WS": "WH",
# MAGIC     "GELB": "YE",
# MAGIC     "YELLOW": "YE",
# MAGIC     "GE": "YE",
# MAGIC }
# MAGIC
# MAGIC
# MAGIC def get_colour_revised(c):
# MAGIC     if c:
# MAGIC         c = c.upper().replace("|", "")
# MAGIC     else:
# MAGIC         return None
# MAGIC     if any(c == color for color in colour_modification_enum.keys()):
# MAGIC         c = c.replace(c, colour_modification_enum[c])
# MAGIC         return c
# MAGIC     else:
# MAGIC         for color in colour_modification_enum.keys():
# MAGIC             if color in c:
# MAGIC                 c = c.replace(color, colour_modification_enum[color])
# MAGIC         return c
# MAGIC
# MAGIC
# MAGIC spark.udf.register("getColourRevised", get_colour_revised, StringType())


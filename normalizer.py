from transliterate import translit

def normal(s):
    ns = ''
    s = s.upper()
    if '"' in s:
        s = s.split('"')[1:]
    elif '(' in s:
        s = s.split('(')[:1]
    for item in s:
        ns += item
    to_replace = {" ": "", "!": "", ",": "", ".": "", "-": "", "GIES": "ДЖИС", "SIS": "ЗИС", "ILE": "АЙЛ", "AND": "ЭНД", "HEA": "ХЭ", "WA": "ВЭЙ", "CS": "КС", "CO": "КО", "WE": "ВЕ", "EW": "ЬЮ", "GE": "Ж", "X": "КС", "HI": "ХАЙ", 
                  "MI": "МАЙ", "LY": "ЛИ", "CC": "КЦ",  "CT": "КТ", "CH": "Х", "EQ": "ЭК",  
                    "CK": "К", "ER": "ЭР", "OO": "У", "HA": "ХЭ", "OU": "У", "W": "В", "C": "К"}
    for item in to_replace.keys():
        ns = ns.replace(item, to_replace[item])
    return translit(ns, 'ru')
"""Small library for the parsing of the toString-representaion of simple Java-Types into python."""
import functools as ft

class ParserException(Exception):
    pass
    
def parseLiteral(lit, srep):
    if not srep.startswith(lit):
        raise ParserException
    else:
        return srep[len(lit):]


def parseJavaMap(keyParser, valueParser, srep):
    ds = {}
    srep = parseLiteral("{", srep)
    
    try:
        srep, key = keyParser(srep)
        srep = parseLiteral("=", srep)
        srep, value = valueParser(srep)
        ds[key] = value
        while True:
            srep = parseLiteral(", ", srep)
            srep, key = keyParser(srep)
            srep = parseLiteral("=", srep)
            srep, value = valueParser(srep)
            ds[key] = value
    except ParserException:
        pass
    srep = parseLiteral("}", srep)
    
    return srep, ds

def mapParser(keyParser, valueParser):
    return ft.partial(parseJavaMap, keyParser, valueParser)
    
def parseJavaList(elementParser, srep):
    ps = []
    srep = parseLiteral("[", srep)
    
    try:
        srep, elem = elementParser(srep)
        ps.append(elem)
        while True:
            srep = parseLiteral(", ", srep)
            srep, elem = elementParser(srep)
            ps.append(elem)
    except ParserException:
        pass
    srep = parseLiteral("]", srep)
    
    return srep, ps
    
def listParser(elementParser):
    return ft.partial(parseJavaList, elementParser)
    
def greedyIntParser(srep):
    last_parse = None
    i = 0
    try:
        while i < len(srep):
            i += 1
            last_parse = int(srep[:i])
    except Exception as e:
        # print("Exception at i=", i ,"; last_parse=", last_parse ," : ", e, sep="") # debug
        pass
    if last_parse is None:
        raise ParserException()
    return srep[i-1:], last_parse
        
def greedyNumberParser(srep):
    last_parse = None
    i = 0
    try:
        while i < len(srep):
            i += 1
            last_parse = float(srep[:i])
    except Exception as e:
        # print("Exception at i=", i ,"; last_parse=", last_parse ," : ", e, sep="") # debug
        pass
    if last_parse is None:
        raise ParserException()
    return srep[i-1:], last_parse


def parseXXLPair(e1parser, e2parser, srep):
    x,y = None, None
    srep = parseLiteral("<", srep)
    srep, x = e1parser(srep)
    srep = parseLiteral(", ", srep)
    srep, y = e2parser(srep)
    srep = parseLiteral(">", srep)
    return srep, (x,y)
    
def pairParser(e1parser, e2parser):
    return ft.partial(parseXXLPair, e1parser, e2parser)
        
        
sre1 = "<{257=[4155284], 727=[4386628], 771=[4245302], 780=[4154669], 915=[4377104], 939=[4741172], 1420=[5340269], 1582=[5617698], 1671=[5395878], 1847=[5743048], 1874=[5430288], 1942=[5537511], 1954=[5411240], 1980=[5798042], 1981=[5722156], 2071=[5433053], 2145=[6571954], 2213=[5422300], 2267=[5456710], 2268=[6107115], 2358=[6063182], 2385=[5903421], 2409=[5903422], 2522=[5673613], 2610=[6054272], 2667=[5959031], 3132=[6473026], 3215=[6898232], 3260=[6649376], 3364=[6789780], 3432=[7710240], 3726=[6632171], 3736=[6770425], 3798=[7614078], 3987=[6651526], 4072=[6732635], 4608=[7531125], 4611=[7377818], 4616=[7469065], 4620=[7370137], 4663=[6987943], 4719=[7837434], 4861=[7585505], 5004=[8014398], 5049=[7510848], 5509=[7745572], 5678=[7548638], 7149=[8769569], 8220=[21493494], 8734=[9004907]}, {257=[594489], 727=[574827], 771=[548712], 780=[542875], 915=[425206], 939=[502014], 1420=[916466], 1582=[726599], 1671=[2016656], 1847=[539188], 1874=[482658], 1942=[531814], 1954=[461152], 1980=[526900], 1981=[1071925], 2071=[559773], 2145=[1068853], 2213=[623370], 2267=[641496], 2268=[2023109], 2358=[687273], 2385=[630743], 2409=[500170], 2522=[624598], 2610=[710008], 2667=[606165], 3132=[761623], 3215=[838123], 3260=[951491], 3364=[567761], 3432=[2180103], 3726=[493103], 3736=[2109133], 3798=[851334], 3987=[597254], 4072=[505393], 4608=[779135], 4611=[623062], 4616=[631665], 4620=[755785], 4663=[737045], 4719=[2266127], 4861=[586195], 5004=[1223390], 5049=[658393], 5509=[605550], 5678=[632586], 7149=[632586], 8220=[2555845], 8734=[756707]}>"

javamap1 = "{257=[4155284], 727=[4386628], 771=[4245302], 780=[4154669], 915=[4377104], 939=[4741172], 1420=[5340269], 1582=[5617698], 1671=[5395878], 1847=[5743048], 1874=[5430288], 1942=[5537511], 1954=[5411240], 1980=[5798042], 1981=[5722156], 2071=[5433053], 2145=[6571954], 2213=[5422300], 2267=[5456710], 2268=[6107115], 2358=[6063182], 2385=[5903421], 2409=[5903422], 2522=[5673613], 2610=[6054272], 2667=[5959031], 3132=[6473026], 3215=[6898232], 3260=[6649376], 3364=[6789780], 3432=[7710240], 3726=[6632171], 3736=[6770425], 3798=[7614078], 3987=[6651526], 4072=[6732635], 4608=[7531125], 4611=[7377818], 4616=[7469065], 4620=[7370137], 4663=[6987943], 4719=[7837434], 4861=[7585505], 5004=[8014398], 5049=[7510848], 5509=[7745572], 5678=[7548638], 7149=[8769569], 8220=[21493494], 8734=[9004907]}"
# parseJavaMap(greedyIntParser, listParser(greedyIntParser), javamap1)[1]

pair_simple = "<1335, [2, 17, 39]>"
# pairParser(greedyIntParser, listParser(greedyIntParser))(pair_simple)
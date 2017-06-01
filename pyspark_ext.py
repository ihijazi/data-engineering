import sys
import time
import json
from datetime import datetime,date
from operator import is_not
from functools import partial
from pyspark.sql import *
class NoneRddElement(object):
  def __getitem__(self,key): 
    return None
  def __getattr__(self, key): 
    if key.startswith('__') and key.endswith('__'):
      return super(NoneRddElement, self).__getAttr__(key)
    return None
  def __getstate__(self): 
    pass
  def __setstate__(self, dict): 
    pass
  def __iter__(self): 
    return iter([NoneRddElement(),NoneRddElement()]);
  def __hash__(self): 
    return 0
  def __eq__(self,other): 
    return True if isinstance(other, NoneRddElement) else False
def sparkVersionNum(ver) :
  v = ver.strip()
  if (len(v) > 5) : v = v[0:5]
  return reduce(lambda sum, elem: sum*10 + elem, map(lambda x: int(x) if x.isdigit() else 0, v.split('.')), 0)
def convert_to_none(x): 
  return NoneRddElement() if x is None else x
def dict2Tuple(t):
  return tuple(map(dict2Tuple, t)) if isinstance(t, (list, tuple)) else tuple(sorted(t.items()))
def tuple2Dict(t):
  return dict((x,y) for x,y in t) if not isinstance(t[0][0], (list, tuple)) else tuple(map(tuple2Dict, t))
def SUM(x): 
  return sum(filter(None,x))
def MAX(x): 
  return max(x)
def MIN(x): 
  return min(x)
def AVG(x): 
  return None if COUNT(x) == 0 else SUM(x)/COUNT(x)
def COUNT(x): 
  return len(filter(partial(is_not, None),x))
def FIRST(x): 
  return x[0]
def LAST(x): 
  return x[-1]
def safeAggregate(x,y): 
  return None if not y else x(y)
def aggFuncExpr(expr, default):
  try: return expr()
  except TypeError as e:
    if 'object is not iterable' in e.args[0]: raise
    else: return default
  except Exception:
    return default
def getValue(type,value,format='%Y-%m-%d'):
  try:
    if type is date:
      return datetime.strptime(value,format).date()
    else: return type(value)
  except ValueError: return None
def getScaledValue(scale, value):
  try: 
    return '' if value is None else ('%0.'+ str(scale) +'f')%float(value)
  except ValueError:
    return ''
def getStrValue(value, format='%Y-%m-%d'):
  if value is None : return ''
  if isinstance(value, date): return value.strftime(format)
  if isinstance(value, str): return unicode(value, 'utf-8')
  if isinstance(value, unicode) : return value
  try: return unicode(value) 
  except UnicodeError : return ''
def safeExpr(expr, default):
  try: return expr()
  except Exception: return default
def getSqlContextInstance(sparkContext): 
  if ('sqlContextSingletonInstance' not in globals()): 
     globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext) 
  return globals()['sqlContextSingletonInstance']
  
import os, sys  # isortのルールに違反
from typing import List


def myFunction(a,b)->List[str]:  # 型ヒントの間違い（ANN系）, 命名規則違反（N802）, 引数間スペースなし（E231）
  """This function does something.
  Args:
    a: first value
    b: second value
  Returns:
    A list of strings
  """  # D213のルールに違反（summaryの位置）
  return [str(a), str(b)]


class  my_class:  # 命名規則違反（N801）、クラス前の空行ルール（D203）
    def __init__(self, value):
        self.value=value  # E225（演算子の前後に空白が必要）


def unused_function():  # 未使用関数（F401相当）
    pass


if __name__=="__main__": print(myFunction(1,2))  # E701（複数文を1行に書いている）
from pyflink.table import DataTypes

from pyflink.table.udf import udf


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())

def func1(s: str):

   return s.replace('World', 'Hey!! Hooray!!')

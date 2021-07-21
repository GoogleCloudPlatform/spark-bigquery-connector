from pyspark.sql.types import UserDefinedType, StringType
from decimal import *

class BigNumericUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for BigNumeric.
    """

    @classmethod
    def sqlType(cls):
        return StringType()

    @classmethod
    def module(cls):
        return "google.cloud.spark.bigquery.big_numeric_support"

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.bigquery.BigNumericUDT"

    def serialize(self, obj):
        if isinstance(obj, BigNumeric):
            return str(obj.number)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        return BigNumeric(Decimal(str(datum)))

    def simpleString(self):
        return "BigNumeric"


class BigNumeric():

    def __init__(self, number):
        if isinstance(number, Decimal):
            self.number = number
        else:
            raise TypeError("BigNumeric Constuctor: expecting Decimal but got %r " % type(number))

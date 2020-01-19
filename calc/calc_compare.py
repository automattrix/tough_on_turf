import pandas as pd



def order_group(d):
    d_sort = sorted(d, key=lambda x: x[1])


def compare_rotation(d1, d2):
    print("Comparing body and head rotation")
    # TODO sort by fastest direction change per sec
    # TODO sort by biggest change in direction
    # TODO sort by longest change in direction

    # d1 head
    # d2 body

    #print(d1.keys())
    print(d1['groups'])

    d1_order_group = ''
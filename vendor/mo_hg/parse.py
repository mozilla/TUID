from jx_base import DataClass
    TODO: WE SHOULD BE ABLE TO STREAM THE RAW DIFF SO WE HANDLE LARGE ONES
                    changes.append(Action(line=int(c[0]), action=d))
                c = MOVE[d](c)


Action = DataClass(
    "Action",
    ["line", "action"],
    constraint=True  # TODO: remove when constrain=None is the same as True
)
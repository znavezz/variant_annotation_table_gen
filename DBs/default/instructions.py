from DBs.default.annotations_func import isAdARFixable
from DBs.default.pre_process import pre_process

instructions = {
    "annotations": {
        "isADARFixable": {
            "type": "bool",
            "description": "Indicates if the variant is ADAR fixable",
            "required": True,
            "compute_function": isAdARFixable,
        },
        # Add more annotations as needed
    },
    "pre_processor": pre_process,
}
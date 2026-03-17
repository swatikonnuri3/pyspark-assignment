import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_1'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'sub_qn_5'))

import util as sub1
from util import apply_mask


def get_final_output(spark):
    """Returns DataFrame with card_number and masked_card_number columns."""
    df = sub1.create_credit_card_df(spark)
    return apply_mask(df).select("card_number", "masked_card_number")
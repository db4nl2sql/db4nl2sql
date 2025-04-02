from util import util, const
import json


class SchemaLinker:
    def __init__(self, setting, logger, rank, llm):
        self.setting = setting
        self.logger = logger
        self.rank = rank
        self.llm = llm

    def get_refined_schema_info(self, db_id: str, mode:str):
        db = self.setting.schema[db_id]
        schema_text = ""

        # generate schema text based on the relevant tables and relevant columns
        if mode == "baseline":
            schema_text = util.make_schema_text(db)
        elif mode == "baseline-w-desc":
            schema_text = util.make_schema_text_w_desc(db) 

        return schema_text

    def get_view_info(self, db_id:str, qnl:str):
        schema_text = ""
        db = self.setting.schema[db_id]

        renamed_view_info = self.setting.renamed_view[db_id]
        cv_flag = False
        cv_result = None

        # make schema text based on the retrieved information
        schema_text = util.make_schema_text_with_view(db, renamed_view_info, cv_flag, cv_result)
        view_type = "cv" if cv_flag else "rv"
        return schema_text, view_type

    def get_unified_view_info(self, db_id:str):
        schema_text = self.setting.unified_view[db_id]["schema_text"]
        return schema_text, 'uv'
    


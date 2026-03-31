import logging
import os
import pandas as pd
from thefuzz import fuzz
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class AutoMapper:
    def __init__(self, threshold: int = 85):
        # batas kemiripan
        self.threshold = threshold
    
    def generate_mapping(self, df_ready: pd.DataFrame, new_catalog: List[Dict[str, Any]]) -> pd.DataFrame:
        logger.info("Memulai proses pencocokan otomatis...")

        required_cols = {"Dataset_Id", "Judul_Tabel"}
        missing = required_cols - set(df_ready.columns)
        if missing:
            raise ValueError(f"df_ready missing required columns: {missing}")

        old_datasets = df_ready[["Dataset_Id", "Judul_Tabel"]].drop_duplicates()

        mapping_data = []
        unmapped_data = []

        for _, row in old_datasets.iterrows():
            old_id = str(row["Dataset_Id"])
            old_title = str(row["Judul_Tabel"])

            best_match_id = None
            best_match_title = None
            highest_score = 0

            # compare new catalog with old catalog
            for new_item in new_catalog:
                new_id = new_item.get("id")
                new_title = str(new_item.get("judul", ""))

                score = fuzz.token_sort_ratio(old_title.lower(), new_title.lower())

                if score > highest_score:
                    highest_score = score
                    best_match_id = new_id
                    best_match_title = new_title

            # evaluasi score kecocokan tertinggi
            if highest_score >= self.threshold:
                mapping_data.append({
                    'old_id': old_id,
                    'old_title': old_title,
                    'new_id': best_match_id,
                    'new_title': best_match_title,
                    'match_score': highest_score
                })
            else:
                unmapped_data.append({
                    'old_id': old_id,
                    'old_title': old_title,
                    'best_new_title_found': best_match_title,
                    'highest_score': highest_score
                })
        
        # report
        output_dir = 'data/reports'
        os.makedirs(output_dir, exist_ok=True)

        if unmapped_data:
            df_unmapped = pd.DataFrame(unmapped_data)
            unmapped_path = f'{output_dir}/unmapped_datasets.csv'
            df_unmapped.to_csv(unmapped_path, index=False)
            logger.warning(
                f"Ditemukan {len(unmapped_data)} dataset yang 'rumahnya' belum dibuat. "
                f"Cek '{unmapped_path}'"
            )
        
        logger.info(f"Berhasil memetakan {len(mapping_data)} dataset.")
        
        df_mapping = pd.DataFrame(mapping_data)
        if not df_mapping.empty:
            # Deteksi jika 1 new_id di-mapping dari >1 old_id (collision)
            dup_new_ids = df_mapping[df_mapping.duplicated(subset=["new_id"], keep=False)]
            if not dup_new_ids.empty:
                logger.warning(
                    f"PERHATIAN: {len(dup_new_ids)} baris mapping menunjuk ke new_id yang sama. "
                    f"Ini bisa menyebabkan data dobel di sistem target!\n"
                    f"{dup_new_ids[['old_id', 'old_title', 'new_id', 'new_title']].to_string()}"
                )
            df_mapping.to_csv(f'{output_dir}/auto_mapping_result.csv', index=False)

        return df_mapping
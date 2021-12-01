
            SELECT id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title FROM tbl_wayadressing 
            ORDER BY id ASC OFFSET {offset} LIMIT {limit}
        
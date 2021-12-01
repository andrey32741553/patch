
            SELECT id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title FROM tbl_stdonechildsc 
            ORDER BY id ASC OFFSET {offset} LIMIT {limit}
        
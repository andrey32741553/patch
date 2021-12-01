
            SELECT id, removal_date, removed, code, color, folder, pos, priority_level, parent, title_en, title_client, title FROM tbl_priority 
            ORDER BY id ASC OFFSET {offset} LIMIT {limit}
        
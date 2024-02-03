use std::{
    cmp::Ordering,
    collections::HashMap,
    io::{self, Read},
    slice::Iter,
};
macro_rules! judge_push_token {
    ($x:expr,$y:expr) => {
        if !$x.is_empty() {
            $y.push($x);
            $x = String::new();
        }
    };
}
macro_rules! data_error {
    () => {
        println!("ERROR");
        return;
    };
}
macro_rules! require_token {
    ($tokens:expr,$dest:expr) => {
        if let Some(s) = $tokens.next() {
            if s != $dest {
                return None;
            }
        } else {
            return None;
        }
    };
}
macro_rules! get_token {
    ($src:expr) => {
        if let Some(s) = $src.next() {
            s.to_string()
        } else {
            return None;
        }
    };
}
macro_rules! judge_name {
    ($n:expr) => {
        if LiteralType::Variable != get_literal_type($n) {
            return None;
        }
    };
}
macro_rules! judge_return {
    ($tokens:expr,$res:expr) => {
        if let None = $tokens.next() {
            Some($res)
        } else {
            None
        }
    };
}
macro_rules! generate_order_value {
    ($raw:expr) => {{
        let (kind, val) = generate_content($raw);
        if kind == LiteralType::Unknown || kind == LiteralType::Null {
            return None;
        } else {
            OrderValue { kind, val }
        }
    }};
}
macro_rules! judge_next_order {
    ($t:expr) => {
        match $t.next() {
            Some(sep) => {
                if sep.as_str() != "," {
                    return None;
                }
                false
            }
            None => true,
        }
    };
}

static KEYWORDS: [&str; 24] = [
    "INT", "CHAR", "CREATE", "TABLE", "PRIMARY", "KEY", "NOT", "NULL", "UNIQUE", "INSERT", "INTO", "VALUES", "DELETE", "FROM", "WHERE", "UPDATE", "ORDER", "BY", "ASC", "DESC", "AND",
    "OR", "BETWEEN", "IS",
];

#[derive(PartialEq)]
enum FieldType {
    Int,
    Chars(usize),
}
#[derive(PartialEq, Clone)]
enum LiteralType {
    Unknown,
    Int,
    Chars,
    Null,
    Variable,
}
#[derive(PartialEq, Clone)]
enum Content {
    Int(i32),
    Chars(String),
    Null,
}
enum ConditionConnector {
    And,
    Or,
}
enum ConditionBlockType {
    Unknown,
    Simple(OrderValue, String, OrderValue),
    Complex(Vec<Condition>),
}
enum OrderSequence {
    ASC,
    DESC,
}
struct Table {
    fields_name: HashMap<String, usize>,
    fields: Vec<Field>,
    records: Vec<Record>,
}
struct Field {
    field_type: FieldType,
    primary_key: bool,
    unique: bool,
    not_null: bool,
}
struct FieldWhileCreate {
    name: String,
    index: usize,
    inner: Field,
}
struct Record {
    contents: Vec<Content>,
}
struct CreateInfo {
    name: String,
    fields: Vec<FieldWhileCreate>,
}
struct InsertInfo {
    dest: String,
    contents: Vec<Content>,
}
struct DeleteInfo {
    dest: String,
    conditions: Vec<Condition>,
}
struct UpdateInfo {
    dest: String,
    updates: Vec<FieldValPair>,
    conditions: Vec<Condition>,
}
struct FieldValPair {
    field_name: String,
    val: Content,
}
struct BackupInfo {
    record_index: usize,
    content_index: usize,
    origin: Content,
}
struct SelectInfo {
    dest: String,
    selected: Vec<String>,
    conditions: Vec<Condition>,
}
struct OrderInfo<T> {
    field: T,
    rule: OrderSequence,
}
#[derive(Clone)]
struct OrderValue {
    kind: LiteralType,
    val: Content,
}
struct Condition {
    connector: ConditionConnector,
    opposite: bool,
    kind: ConditionBlockType,
}

impl PartialEq for FieldWhileCreate {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
fn main() {
    let mut buffer = String::new();
    let mut tables: HashMap<String, Table> = HashMap::new();
    io::stdin().read_to_string(&mut buffer).unwrap();
    let sentences = buffer.trim().split(';');
    for sentence in sentences {
        let tokens = parse_token(sentence);
        if let Some(tokens) = tokens {
            if tokens.is_empty() {
                continue;
            }
            // for token in tokens {
            //     print!("{} ", token);
            // }
            // println!();
            match tokens[0].as_str() {
                "CREATE" => {
                    create_table(&mut tables, tokens);
                }
                "INSERT" => {
                    insert_element(&mut tables, tokens);
                }
                "DELETE" => {
                    delete_element(&mut tables, tokens);
                }
                "UPDATE" => {
                    update_element(&mut tables, tokens);
                }
                "SELECT" => {
                    select_element(&mut tables, tokens);
                }
                _ => {
                    println!("SYNTAX ERROR")
                }
            }
        } else {
            println!("SYNTAX ERROR")
        }
    }
}

fn create_table(tables: &mut HashMap<String, Table>, tokens: Vec<String>) {
    if let Some(ci) = parse_create(&tokens) {
        if tables.contains_key(&ci.name) {
            data_error!();
        }
        let mut table = Table {
            fields_name: HashMap::new(),
            fields: Vec::new(),
            records: Vec::new(),
        };
        let mut primary_count = 0;
        for i in 0..ci.fields.len() {
            if ci.fields[i].inner.primary_key {
                primary_count += 1;
            }
            if ci.fields[0..i].contains(&ci.fields[i]) {
                data_error!();
            }
        }
        if primary_count != 1 {
            data_error!();
        }
        for f in ci.fields {
            table.fields_name.insert(f.name, f.index);
            table.fields.push(f.inner);
        }
        tables.insert(ci.name, table);
        println!("CREATE TABLE SUCCESSFULLY")
    } else {
        println!("SYNTAX ERROR")
    }
}
fn parse_create(tokens: &Vec<String>) -> Option<CreateInfo> {
    let mut token_iter = tokens.iter();
    token_iter.next().unwrap();
    require_token!(token_iter, "TABLE");
    let mut ci = CreateInfo {
        name: get_token!(token_iter),
        fields: Vec::new(),
    };
    judge_name!(&ci.name);
    require_token!(token_iter, "(");
    let mut field_count: usize = 0;
    'prop_process: loop {
        let name = get_token!(token_iter);
        judge_name!(&name);
        let ty = get_token!(token_iter);
        let mut f = FieldWhileCreate {
            name,
            index: field_count,
            inner: Field {
                field_type: match ty.as_str() {
                    "INT" => FieldType::Int,
                    "CHAR" => {
                        require_token!(token_iter, "(");
                        let len = get_token!(token_iter);
                        require_token!(token_iter, ")");
                        FieldType::Chars(len.parse().unwrap())
                    }
                    _ => return None,
                },
                primary_key: false,
                unique: false,
                not_null: false,
            },
        };
        let mut prop_stage = 0;
        field_count += 1;
        loop {
            let prop = get_token!(token_iter);
            match prop.as_str() {
                "NOT" => {
                    if prop_stage >= 1 {
                        return None;
                    }
                    require_token!(token_iter, "NULL");
                    f.inner.not_null = true;
                    prop_stage = 1;
                }
                "PRIMARY" => {
                    if prop_stage >= 2 {
                        return None;
                    }
                    require_token!(token_iter, "KEY");
                    f.inner.primary_key = true;
                    f.inner.not_null = true;
                    f.inner.unique = true;
                    prop_stage = 2;
                }
                "UNIQUE" => {
                    if prop_stage >= 3 {
                        return None;
                    }
                    f.inner.unique = true;
                    prop_stage = 3
                }
                "," => {
                    ci.fields.push(f);
                    continue 'prop_process;
                }
                ")" => {
                    ci.fields.push(f);
                    break 'prop_process;
                }
                _ => return None,
            }
        }
    }
    judge_return!(token_iter, ci)
}

fn insert_element(tables: &mut HashMap<String, Table>, tokens: Vec<String>) {
    if let Some(ii) = parse_insert(&tokens) {
        if !tables.contains_key(&ii.dest) {
            data_error!();
        }
        let dest = tables.get_mut(&ii.dest).unwrap();
        if dest.fields.len() != ii.contents.len() {
            data_error!();
        }
        for i in 0..dest.fields.len() {
            let dest_field = &dest.fields[i];
            match &ii.contents[i] {
                Content::Int(_) => {
                    if FieldType::Int != dest_field.field_type {
                        data_error!();
                    }
                }
                Content::Chars(s) => {
                    if let FieldType::Chars(max) = dest_field.field_type {
                        if max < s.len() - 2 {
                            data_error!();
                        }
                    } else {
                        data_error!();
                    }
                }
                Content::Null => {
                    if dest_field.not_null {
                        data_error!();
                    }
                }
            }
            if dest_field.unique && ii.contents[i] != Content::Null {
                for r in &dest.records {
                    if r.contents[i] == ii.contents[i] {
                        data_error!();
                    }
                }
            }
        }
        dest.records.push(Record {
            contents: ii.contents,
        });
        println!("1 RECORDS INSERTED")
    } else {
        println!("SYNTAX ERROR")
    }
}
fn parse_insert(tokens: &Vec<String>) -> Option<InsertInfo> {
    let mut token_iter = tokens.iter();
    token_iter.next().unwrap();
    require_token!(token_iter, "INTO");
    let mut ii = InsertInfo {
        dest: get_token!(token_iter),
        contents: Vec::new(),
    };
    judge_name!(&ii.dest);
    require_token!(token_iter, "VALUES");
    require_token!(token_iter, "(");
    loop {
        let v = get_token!(token_iter);
        let (kind, val) = generate_content(v);
        if kind == LiteralType::Unknown || kind == LiteralType::Variable {
            return None;
        }
        ii.contents.push(val);
        let sep = get_token!(token_iter);
        match sep.as_str() {
            "," => continue,
            ")" => break,
            _ => return None,
        }
    }
    judge_return!(token_iter, ii)
}

fn delete_element(tables: &mut HashMap<String, Table>, tokens: Vec<String>) {
    if let Some(di) = parse_delete(&tokens) {
        if !tables.contains_key(&di.dest) {
            data_error!();
        }
        let dest = &mut tables.get_mut(&di.dest).unwrap();
        let origin_len = dest.records.len();
        let mut count = 0;
        for i in (0..origin_len).rev() {
            if let Some(result) = judge_condition(&dest.records[i], &dest.fields_name, &dest.fields, &di.conditions) {
                if result {
                    count += 1;
                    dest.records.remove(i);
                }
            } else {
                data_error!();
            }
        }
        println!("{} RECORDS DELETED", count);
    } else {
        println!("SYNTAX ERROR")
    }
}
fn parse_delete(tokens: &Vec<String>) -> Option<DeleteInfo> {
    let mut token_iter = tokens.iter();
    token_iter.next().unwrap();
    require_token!(token_iter, "FROM");
    let mut di = DeleteInfo {
        dest: get_token!(token_iter),
        conditions: Vec::new(),
    };
    judge_name!(&di.dest);
    require_token!(token_iter, "WHERE");
    if let Some(conditions) = parse_condition(&mut token_iter, true) {
        di.conditions = conditions;
        return Some(di);
    }
    None
}

fn update_element(tables: &mut HashMap<String, Table>, tokens: Vec<String>) {
    if let Some(ui) = parse_update(&tokens) {
        if !tables.contains_key(&ui.dest) {
            data_error!();
        }
        let dest = tables.get_mut(&ui.dest).unwrap();
        let mut count = 0;
        let mut backups: Vec<BackupInfo> = Vec::new();
        for i in 0..dest.records.len() {
            let r = &mut dest.records[i];
            if let Some(res) = judge_condition(&r, &dest.fields_name, &dest.fields, &ui.conditions) {
                if res {
                    count += 1;
                    for update in &ui.updates {
                        if !dest.fields_name.contains_key(&update.field_name) {
                            data_error!();
                        }
                        let index = dest.fields_name[&update.field_name];
                        let field = &dest.fields[index];
                        if update.val == Content::Null {
                            if field.not_null {
                                data_error!();
                            }
                        } else {
                            match (&field.field_type, &update.val) {
                                (FieldType::Int, Content::Int(_)) => {}
                                (FieldType::Chars(len), Content::Chars(val)) => {
                                    if val.len() - 2 > *len {
                                        data_error!();
                                    }
                                }
                                _ => {
                                    data_error!();
                                }
                            }
                        }
                        backups.push(BackupInfo {
                            record_index: i,
                            content_index: index,
                            origin: r.contents[index].clone(),
                        });
                        r.contents[index] = update.val.clone();
                    }
                }
            } else {
                data_error!();
            }
        }
        let mut conflit = false;
        if count != 0 {
            'outer: for index in 0..dest.fields.len() {
                if !dest.fields[index].unique {
                    continue;
                }
                let column: Vec<&Content> = dest.records.iter().map(|x| &x.contents[index]).collect();
                let len = column.len();
                for i in 0..len {
                    if let Content::Null = column[i] {
                        continue;
                    }
                    for j in i + 1..len {
                        if column[i] == column[j] {
                            conflit = true;
                            break 'outer;
                        }
                    }
                }
            }
        }
        if !conflit {
            println!("{} RECORDS UPDATED", count);
        } else {
            for backup in backups {
                dest.records[backup.record_index].contents[backup.content_index] = backup.origin
            }
            data_error!();
        }
    } else {
        println!("SYNTAX ERROR")
    }
}
fn parse_update(tokens: &Vec<String>) -> Option<UpdateInfo> {
    let mut token_iter = tokens.iter();
    token_iter.next().unwrap();
    let mut ui = UpdateInfo {
        dest: get_token!(token_iter),
        updates: Vec::new(),
        conditions: Vec::new(),
    };
    judge_name!(&ui.dest);
    require_token!(token_iter, "SET");
    loop {
        let field_name = get_token!(token_iter);
        judge_name!(&field_name);
        require_token!(token_iter, "=");
        let raw_val = get_token!(token_iter);
        let (kind, val) = generate_content(raw_val);
        if kind == LiteralType::Unknown || kind == LiteralType::Variable {
            return None;
        }
        ui.updates.push(FieldValPair {
            field_name,
            val,
        });
        let temp = get_token!(token_iter);
        match temp.as_str() {
            "WHERE" => break,
            "," => continue,
            _ => return None,
        }
    }
    if let Some(conditions) = parse_condition(&mut token_iter, true) {
        ui.conditions = conditions;
        return Some(ui);
    }
    None
}

fn select_element(tables: &mut HashMap<String, Table>, tokens: Vec<String>) {
    let mut sep_iter = tokens.split(|x| x == "ORDER");
    let main = sep_iter.next().unwrap().iter();
    let orders = if let Some(raw_orders) = sep_iter.next() {
        if let Some(orders) = parse_order(raw_orders.iter()) {
            orders
        } else {
            println!("SYNTAX ERROR");
            return;
        }
    } else {
        Vec::new()
    };
    if let Some(si) = parse_select(main) {
        if !tables.contains_key(&si.dest) {
            data_error!();
        }
        let dest = tables.get_mut(&si.dest).unwrap();
        let mut selected = Vec::new();
        if si.selected.len() == 0 {
            selected = vec!["".to_string(); dest.fields.len()];
            for (field, i) in &dest.fields_name {
                selected[*i] = field.clone();
            }
        } else {
            for field in si.selected {
                if let Some(_) = dest.fields_name.get(&field) {
                    selected.push(field);
                } else {
                    data_error!();
                }
            }
        }
        let mut parse_order = Vec::new();
        for o in orders {
            if !dest.fields_name.contains_key(&o.field) {
                data_error!();
            }
            parse_order.push(OrderInfo {
                field: dest.fields_name[&o.field],
                rule: o.rule,
            })
        }
        for i in 0..dest.fields.len() {
            if dest.fields[i].primary_key {
                parse_order.push(OrderInfo {
                    field: i,
                    rule: OrderSequence::ASC,
                });
                break;
            }
        }
        let mut selected_record = Vec::new();
        for r in &dest.records {
            if let Some(res) = judge_condition(r, &dest.fields_name, &dest.fields, &si.conditions) {
                if res {
                    selected_record.push(r);
                }
            } else {
                data_error!();
            }
        }
        println!("{} RECORDS FOUND", selected_record.len());
        if selected_record.is_empty() {
            return;
        }
        selected_record.sort_by(|l, r| sort_record(*l, *r, &parse_order));
        for field_name in &selected {
            print!("{}\t", *field_name);
        }
        println!();
        for record in selected_record {
            for name in &selected {
                print!(
                    "{}\t",
                    match &record.contents[dest.fields_name[name]] {
                        Content::Int(v) => v.to_string(),
                        Content::Chars(v) => v.to_string(),
                        Content::Null => "NULL".to_string(),
                    }
                )
            }
            println!();
        }
    } else {
        println!("SYNTAX ERROR")
    }
}
fn parse_select(mut tokens: Iter<'_, String>) -> Option<SelectInfo> {
    tokens.next().unwrap();
    let mut selected = Vec::new();
    let mut select_all = false;
    loop {
        let mut temp = get_token!(tokens);
        if temp == "*" {
            select_all = true;
            if !selected.is_empty() {
                return None;
            }
        } else {
            if select_all {
                return None;
            }
            judge_name!(&temp);
            selected.push(temp);
        }
        temp = get_token!(tokens);
        match temp.as_str() {
            "FROM" => break,
            "," => continue,
            _ => return None,
        }
    }
    Some(SelectInfo {
        dest: get_token!(tokens),
        selected,
        conditions: if let Some(test) = tokens.next() {
            if test != "WHERE" {
                return None;
            }
            if let Some(cond) = parse_condition(&mut tokens, true) {
                cond
            } else {
                return None;
            }
        } else {
            Vec::new()
        },
    })
}
fn parse_order(mut tokens: Iter<'_, String>) -> Option<Vec<OrderInfo<String>>> {
    require_token!(tokens, "BY");
    let mut orders = Vec::new();
    loop {
        let mut oi = OrderInfo {
            field: get_token!(tokens),
            rule: OrderSequence::ASC,
        };
        let last = match tokens.next() {
            Some(test) => match test.as_str() {
                "ASC" => {
                    oi.rule = OrderSequence::ASC;
                    judge_next_order!(tokens)
                }
                "DESC" => {
                    oi.rule = OrderSequence::DESC;
                    judge_next_order!(tokens)
                }
                "," => false,
                _ => return None,
            },
            None => true,
        };
        orders.push(oi);
        if last {
            break;
        }
    }
    Some(orders)
}
fn sort_record(lhs: &Record, rhs: &Record, rules: &Vec<OrderInfo<usize>>) -> Ordering {
    let mut result: Ordering = Ordering::Equal;
    for rule in rules {
        if result != Ordering::Equal {
            return result;
        }
        result = match (&lhs.contents[rule.field], &rhs.contents[rule.field]) {
            (Content::Int(l), Content::Int(r)) => {
                if let OrderSequence::ASC = rule.rule {
                    l.cmp(&r)
                } else {
                    r.cmp(&l)
                }
            }
            (Content::Chars(l), Content::Chars(r)) => {
                if let OrderSequence::ASC = rule.rule {
                    l.cmp(&r)
                } else {
                    r.cmp(&l)
                }
            }
            _ => Ordering::Equal,
        }
    }
    result
}

fn parse_condition(tokens: &mut Iter<'_, String>, outer: bool) -> Option<Vec<Condition>> {
    let mut conditions = Vec::new();
    let mut connector = ConditionConnector::And;
    loop {
        let mut condition = Condition {
            connector,
            opposite: false,
            kind: ConditionBlockType::Unknown,
        };
        let mut temp = get_token!(tokens);
        while temp == "NOT" {
            condition.opposite = !condition.opposite;
            temp = get_token!(tokens);
        }
        if temp == "(" {
            condition.kind = if let Some(complex) = parse_condition(tokens, false) {
                ConditionBlockType::Complex(complex)
            } else {
                return None;
            };
            conditions.push(condition);
        } else {
            let lhs = generate_order_value!(temp);
            temp = get_token!(tokens);
            match temp.as_str() {
                "IS" => {
                    let rhs = OrderValue {
                        kind: LiteralType::Null,
                        val: Content::Null,
                    };
                    temp = get_token!(tokens);
                    match temp.as_str() {
                        "NULL" => condition.kind = ConditionBlockType::Simple(lhs, "=".to_string(), rhs),
                        "NOT" => {
                            require_token!(tokens, "NULL");
                            condition.kind = ConditionBlockType::Simple(lhs, "<>".to_string(), rhs)
                        }
                        _ => return None,
                    }
                }
                "BETWEEN" => {
                    temp = get_token!(tokens);
                    let rhs1 = generate_order_value!(temp);
                    require_token!(tokens, "AND");
                    temp = get_token!(tokens);
                    let rhs2 = generate_order_value!(temp);
                    let lhs_dup = lhs.clone();
                    condition.kind = ConditionBlockType::Simple(lhs, ">=".to_string(), rhs1);
                    conditions.push(condition);
                    condition = Condition {
                        connector: ConditionConnector::And,
                        opposite: false,
                        kind: ConditionBlockType::Simple(lhs_dup, "<=".to_string(), rhs2),
                    };
                }
                "=" | ">" | "<" | "<>" | ">=" | "<=" => {
                    let operator = temp;
                    temp = get_token!(tokens);
                    let rhs = generate_order_value!(temp);
                    condition.kind = ConditionBlockType::Simple(lhs, operator, rhs);
                }
                _ => return None,
            }
            conditions.push(condition);
        }
        if let Some(temp) = tokens.next() {
            match temp.as_str() {
                "AND" => {
                    connector = ConditionConnector::And;
                }
                "OR" => {
                    connector = ConditionConnector::Or;
                }
                ")" => {
                    if outer {
                        return None;
                    } else {
                        return Some(conditions);
                    }
                }
                _ => {
                    return None;
                }
            }
        } else {
            if outer {
                return Some(conditions);
            } else {
                return None;
            }
        }
    }
}
fn judge_condition(record: &Record, fields_name: &HashMap<String, usize>, fields: &Vec<Field>, conditions: &Vec<Condition>) -> Option<bool> {
    let mut res = true;
    for cond in conditions {
        let current = match &cond.kind {
            ConditionBlockType::Simple(lft, connector, rgt) => {
                let mut lhs = lft.clone();
                let mut rhs = rgt.clone();
                if !parse_field(&mut lhs, record, fields_name, fields) {
                    return None;
                }
                if !parse_field(&mut rhs, record, fields_name, fields) {
                    return None;
                }
                match connector.as_str() {
                    "=" => match (lhs.kind, rhs.kind) {
                        (LiteralType::Chars, LiteralType::Chars)
                        | (LiteralType::Int, LiteralType::Int)
                        | (LiteralType::Int, LiteralType::Null)
                        | (LiteralType::Chars, LiteralType::Null) => lhs.val == rhs.val,
                        _ => return None,
                    },
                    "<>" => match (lhs.kind, rhs.kind) {
                        (LiteralType::Chars, LiteralType::Chars)
                        | (LiteralType::Int, LiteralType::Int)
                        | (LiteralType::Int, LiteralType::Null)
                        | (LiteralType::Chars, LiteralType::Null) => lhs.val != rhs.val,
                        _ => return None,
                    },
                    "<" => match (lhs.val, rhs.val) {
                        (Content::Int(int_l), Content::Int(int_r)) => int_l < int_r,
                        (Content::Chars(str_l), Content::Chars(str_r)) => str_l < str_r,
                        _ => return None,
                    },
                    ">" => match (lhs.val, rhs.val) {
                        (Content::Int(int_l), Content::Int(int_r)) => int_l > int_r,
                        (Content::Chars(str_l), Content::Chars(str_r)) => str_l > str_r,
                        _ => return None,
                    },
                    "<=" => match (lhs.val, rhs.val) {
                        (Content::Int(int_l), Content::Int(int_r)) => int_l <= int_r,
                        (Content::Chars(str_l), Content::Chars(str_r)) => str_l <= str_r,
                        _ => return None,
                    },
                    ">=" => match (lhs.val, rhs.val) {
                        (Content::Int(int_l), Content::Int(int_r)) => int_l >= int_r,
                        (Content::Chars(str_l), Content::Chars(str_r)) => str_l >= str_r,
                        _ => return None,
                    },
                    _ => return None,
                }
            }
            ConditionBlockType::Complex(conds) => {
                if let Some(rval) = judge_condition(record, fields_name, fields, &conds) {
                    rval
                } else {
                    return None;
                }
            }
            ConditionBlockType::Unknown => return None,
        };
        match cond.connector {
            ConditionConnector::And => res &= current,
            ConditionConnector::Or => res |= current,
        }
    }
    Some(res)
}

fn parse_token(raw: &str) -> Option<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_str = false;
    let mut convert = false;
    let mut i: usize = 0;
    while i < raw.len() {
        let c = raw[i..=i].parse().unwrap();
        if convert {
            convert = false;
            if c == '\\' || c == '\'' {
                current.push(c);
                i += 1;
                continue;
            }
        }
        if in_str {
            if c == '\n' {
                return None;
            }
            current.push(c);
            if c == '\\' {
                convert = true;
            }
            if c == '\'' {
                in_str = false;
                tokens.push(current);
                current = String::new();
            }
        } else {
            match c {
                ' ' | '\n' | '\r' | '\t' => {
                    judge_push_token!(current, tokens);
                }
                '(' | ')' | ',' | '=' => {
                    judge_push_token!(current, tokens);
                    tokens.push(c.to_string());
                }
                '<' => {
                    judge_push_token!(current, tokens);
                    if i + 1 < raw.len() && (&raw[i + 1..=i + 1] == "=" || &raw[i + 1..=i + 1] == ">") {
                        tokens.push(raw[i..=i + 1].to_string());
                        i += 1;
                    } else {
                        tokens.push(c.to_string());
                    }
                }
                '>' => {
                    judge_push_token!(current, tokens);
                    if i + 1 < raw.len() && &raw[i + 1..=i + 1] == "=" {
                        tokens.push(raw[i..=i + 1].to_string());
                        i += 1;
                    } else {
                        tokens.push(c.to_string());
                    }
                }
                '\\' => {
                    convert = true;
                    current.push(c);
                }
                '\'' => {
                    judge_push_token!(current, tokens);
                    in_str = true;
                    current.push(c);
                }
                _ => {
                    current.push(c);
                }
            }
        }
        i += 1;
    }
    judge_push_token!(current, tokens);
    if in_str {
        None
    } else {
        Some(tokens)
    }
}

fn get_literal_type(ltr: &str) -> LiteralType {
    if ltr == "NULL" {
        return LiteralType::Null;
    }
    if KEYWORDS.contains(&ltr) {
        return LiteralType::Unknown;
    }
    let first = ltr[0..=0].parse::<char>().unwrap();
    if first == '\'' {
        return LiteralType::Chars;
    }
    let first_expression = if first.is_ascii_digit() || first == '-' {
        LiteralType::Int
    } else if first.is_ascii_alphabetic() || first == '_' {
        LiteralType::Variable
    } else {
        return LiteralType::Unknown;
    };
    for c in ltr[1..].chars() {
        if let LiteralType::Variable = first_expression {
            if !c.is_ascii_alphabetic() && c != '_' {
                return LiteralType::Unknown;
            }
        } else {
            if !c.is_ascii_digit() {
                return LiteralType::Unknown;
            }
        }
    }
    if let LiteralType::Variable = first_expression {
        LiteralType::Variable
    } else {
        LiteralType::Int
    }
}

fn generate_content(raw: String) -> (LiteralType, Content) {
    let kind = get_literal_type(&raw);
    let content = match kind {
        LiteralType::Unknown | LiteralType::Null => Content::Null,
        LiteralType::Int => Content::Int(raw.parse().unwrap()),
        LiteralType::Chars | LiteralType::Variable => Content::Chars(raw),
    };
    return (kind, content);
}

fn parse_field(origin: &mut OrderValue, record: &Record, fields_name: &HashMap<String, usize>, fields: &Vec<Field>) -> bool {
    if let LiteralType::Variable = origin.kind {
        if let Content::Chars(name) = &origin.val {
            if !fields_name.contains_key(name) {
                return false;
            }
            let index = fields_name[name];
            let field = &fields[index];
            origin.kind = match field.field_type {
                FieldType::Int => LiteralType::Int,
                FieldType::Chars(_) => LiteralType::Chars,
            };
            origin.val = record.contents[index].clone();
        }
    }
    true
}

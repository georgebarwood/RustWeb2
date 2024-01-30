use crate::share::TransExt;
use rustdb::{
    c_int, c_value, check_types, standard_builtins, Block, BuiltinMap, CExp, CExpPtr, CompileFunc,
    DataKind, EvalEnv, Expr, Value,
};
use std::rc::Rc;

/// Get BuiltinMap
pub fn get_bmap() -> BuiltinMap {
    // Construct map of "builtin" functions that can be called in SQL code.
    // Include extra functions ARGON, EMAILTX and SLEEP as well as the standard functions.
    let mut bmap = BuiltinMap::default();
    standard_builtins(&mut bmap);
    let list = [
        ("ARGON", DataKind::Binary, CompileFunc::Value(c_argon)),
        ("EMAILTX", DataKind::Int, CompileFunc::Int(c_email_tx)),
        ("SLEEP", DataKind::Int, CompileFunc::Int(c_sleep)),
        ("SETDOS", DataKind::Int, CompileFunc::Int(c_setdos)),
        ("TRANSWAIT", DataKind::Int, CompileFunc::Int(c_trans_wait)),
        ("TOPDF", DataKind::Int, CompileFunc::Int(c_topdf)),
        ("BINPACK", DataKind::Binary, CompileFunc::Value(c_binpack)),
        (
            "BINUNPACK",
            DataKind::Binary,
            CompileFunc::Value(c_binunpack),
        ),
        ("SETMEM", DataKind::Int, CompileFunc::Int(c_setmem)),
        (
            "DESERIALISE",
            DataKind::String,
            CompileFunc::Value(c_deserialise),
        ),
        ("NOLOG", DataKind::Int, CompileFunc::Int(c_nolog)),
    ];
    for (name, typ, cf) in list {
        bmap.insert(name.to_string(), (typ, cf));
    }
    bmap
}

/// Compile call to ARGON.
fn c_argon(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::String, DataKind::String]);
    let password = c_value(b, &mut args[0]);
    let salt = c_value(b, &mut args[1]);
    Box::new(Argon { password, salt })
}

/// Compiled call to ARGON.
struct Argon {
    password: CExpPtr<Value>,
    salt: CExpPtr<Value>,
}
impl CExp<Value> for Argon {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let pw = self.password.eval(ee, d).str();
        let salt = self.salt.eval(ee, d).str();

        let result = argon2rs::argon2i_simple(&pw, &salt).to_vec();
        Value::RcBinary(Rc::new(result))
    }
}

/// Compile call to SLEEP.
fn c_sleep(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::Int]);
    let to = c_int(b, &mut args[0]);
    Box::new(Sleep { to })
}

/// Compiled call to SLEEP
struct Sleep {
    to: CExpPtr<i64>,
}
impl CExp<i64> for Sleep {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let to = self.to.eval(ee, d);
        let mut ext = ee.tr.get_extension();
        if let Some(ext) = ext.downcast_mut::<TransExt>() {
            ext.sleep = if to <= 0 { 1 } else { to as u64 };
        }
        ee.tr.set_extension(ext);
        0
    }
}

/// Compile call to SETDOS.
fn c_setdos(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(
        b,
        args,
        &[
            DataKind::String,
            DataKind::Int,
            DataKind::Int,
            DataKind::Int,
            DataKind::Int,
        ],
    );
    let uid = c_value(b, &mut args[0]);
    let mut to = Vec::new();
    for i in 0..4 {
        to.push(c_int(b, &mut args[i + 1]));
    }
    Box::new(SetDos { uid, to })
}

/// Compiled call to SETDOS
struct SetDos {
    uid: CExpPtr<Value>,
    to: Vec<CExpPtr<i64>>,
}
impl CExp<i64> for SetDos {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let mut result = 0;
        let uid = self.uid.eval(ee, d).str().to_string();
        let mut to = [0; 4];
        for (i, item) in to.iter_mut().enumerate() {
            *item = self.to[i].eval(ee, d) as u64;
        }
        let mut ext = ee.tr.get_extension();
        if let Some(ext) = ext.downcast_mut::<TransExt>() {
            ext.uid = uid.clone();
            if ext.set_dos(uid, to) {
                result = 1;
            }
        }
        ee.tr.set_extension(ext);
        result
    }
}

/// Compile call to EMAILTX.
fn c_email_tx(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(EmailTx {})
}

/// Compiled call to EMAILTX
struct EmailTx {}
impl CExp<i64> for EmailTx {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        let mut ext = ee.tr.get_extension();
        if let Some(ext) = ext.downcast_mut::<TransExt>() {
            ext.tx_email = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}

/// Compile call to TRANSWAIT.
fn c_trans_wait(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(TransWait {})
}

/// Compiled call to TRANSWAIT
struct TransWait {}
impl CExp<i64> for TransWait {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        let mut ext = ee.tr.get_extension();
        if let Some(ext) = ext.downcast_mut::<TransExt>() {
            ext.trans_wait = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}

/// Compile call to TOPDF
fn c_topdf(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(ToPdf {})
}

/// Compiled call to TOPDF
struct ToPdf {}
impl CExp<i64> for ToPdf {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        let mut ext = ee.tr.get_extension();
        if let Some(ext) = ext.downcast_mut::<TransExt>() {
            ext.to_pdf = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}

/// Compile call to BINPACK.
fn c_binpack(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Binary]);
    let bytes = c_value(b, &mut args[0]);
    Box::new(Binpack { bytes })
}
/// Compiled call to BINPACK.
struct Binpack {
    bytes: CExpPtr<Value>,
}
impl CExp<Value> for Binpack {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        if let Value::RcBinary(data) = self.bytes.eval(ee, d) {
            let cb: Vec<u8> = flate3::deflate(&data);
            Value::RcBinary(Rc::new(cb))
        } else {
            panic!();
        }
    }
}
/// Compile call to BINUNPACK.
fn c_binunpack(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Binary]);
    let bytes = c_value(b, &mut args[0]);
    Box::new(Binunpack { bytes })
}
/// Compiled call to BINUNPACK.
struct Binunpack {
    bytes: CExpPtr<Value>,
}
impl CExp<Value> for Binunpack {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        if let Value::RcBinary(data) = self.bytes.eval(ee, d) {
            let ucb: Vec<u8> = flate3::inflate(&data);
            Value::RcBinary(Rc::new(ucb))
        } else {
            panic!();
        }
    }
}

/// Compile call to SETMEM.
fn c_setmem(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[DataKind::Int]);
    let to = c_int(b, &mut args[0]);
    Box::new(SetMem { to })
}

/// Compiled call to SETMEM
struct SetMem {
    to: CExpPtr<i64>,
}
impl CExp<i64> for SetMem {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> i64 {
        let to = self.to.eval(ee, d) as usize;
        ee.db.apd.spd.stash.lock().unwrap().mem_limit = to * 1024 * 1024;
        0
    }
}

/// Compile call to DESERIALISE.
fn c_deserialise(b: &Block, args: &mut [Expr]) -> CExpPtr<Value> {
    check_types(b, args, &[DataKind::Binary]);
    let ser = c_value(b, &mut args[0]);
    Box::new(Deserialise { ser })
}

/// Compiled call to DESERIALISE
struct Deserialise {
    ser: CExpPtr<Value>,
}

impl CExp<Value> for Deserialise {
    fn eval(&self, ee: &mut EvalEnv, d: &[u8]) -> Value {
        let ser = self.ser.eval(ee, d).bin();
        let qy: rustdb::gentrans::GenQuery = bincode::deserialize(&ser).unwrap();
        let s = serde_json::to_string(&qy).unwrap();
        Value::String(Rc::new(s))
    }
}

/// Compile call to NOLOG.
fn c_nolog(b: &Block, args: &mut [Expr]) -> CExpPtr<i64> {
    check_types(b, args, &[]);
    Box::new(NoLog {})
}

/// Compiled call to NOLOG
struct NoLog {}
impl CExp<i64> for NoLog {
    fn eval(&self, ee: &mut EvalEnv, _d: &[u8]) -> i64 {
        let mut ext = ee.tr.get_extension();
        if let Some(ext) = ext.downcast_mut::<TransExt>() {
            ext.no_log = true;
        }
        ee.tr.set_extension(ext);
        0
    }
}

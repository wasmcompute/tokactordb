use std::marker::PhantomData;

use crc::{Crc, CRC_32_ISCSI};
use tokactor::{
    util::builder::{ActorAskRef, CtxBuilder},
    Actor, Ask, Ctx, DeadActorResult, Handler,
};

use crate::actors::tree::{PrimaryKey, RecordValue};

/// Store a particular version of a tree. This value is saved to the database
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedTree {
    version: u64,
    table_name: String,
    key_name: String,
    value_name: String,
    value_json: Vec<u8>,
    value_bincode: Vec<u8>,
    hash: u32,
    // address: mpsc::Sender<(Option<Vec<u8>>, Option<Vec<u8>>)>,
}

impl VersionedTree {
    /// When creating a new tree, it is required to create a versioned record of
    /// that tree. This includes metadata about the record and is used to hopefully
    /// catch structure mutations before the database starts.
    ///
    /// For now, there is no easy way to do this through types so the check for
    /// a tree data version needs to be done at runtime, this should be ok because
    /// the database restore should happen quickly every time.
    ///
    /// Save the following:
    ///     a) Create a default record
    ///     b) Create a hash of that record
    ///     c) Create multiple serialization of that record
    ///     d) Store all of those values in a database table
    pub fn new<Key: PrimaryKey, Value: RecordValue>(table_name: String) -> anyhow::Result<Self> {
        let default = Value::default();
        let value_name = std::any::type_name::<Value>().to_string();
        let value_json = serde_json::to_vec_pretty(&default)?;
        let value_bincode = bincode::serialize(&default)?;
        let key_name = std::any::type_name::<Key>().to_string();
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&value_json);
        digest.update(&value_bincode);
        digest.update(value_name.as_bytes());
        digest.update(key_name.as_bytes());
        digest.update(table_name.as_bytes());
        let hash = digest.finalize();
        // let (tx, rx) = mpsc::channel();

        Ok(Self {
            version: 0,
            table_name,
            key_name,
            value_name,
            value_json,
            value_bincode,
            hash,
            // address: tx,
        })
    }
}

#[derive(Debug, Clone)]
pub struct UpgradedVersion {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl UpgradedVersion {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }
}

#[derive(Debug, Clone)]
pub struct UpgradeVersion {
    past_key: Vec<u8>,
    past_value: Vec<u8>,
}

impl UpgradeVersion {
    pub fn new(past_key: Vec<u8>, past_value: Vec<u8>) -> Self {
        Self {
            past_key,
            past_value,
        }
    }
}

/// Actor used to upgrade one value of a record to another.
#[derive(Debug)]
pub struct VersionedTreeUpgradeActor<PastKey, PastValue, CurrentKey, CurrentValue>
where
    PastKey: PrimaryKey,
    PastValue: RecordValue,
    CurrentKey: PrimaryKey + From<PastKey>,
    CurrentValue: RecordValue + From<PastValue>,
{
    _past_key: PhantomData<PastKey>,
    _past_value: PhantomData<PastValue>,
    _current_key: PhantomData<CurrentKey>,
    _current_value: PhantomData<CurrentValue>,
}

impl<Pk, Pv, Ck, Cv> Actor for VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>
where
    Pk: PrimaryKey,
    Pv: RecordValue,
    Ck: PrimaryKey + From<Pk>,
    Cv: RecordValue + From<Pv>,
{
}

impl<Pk, Pv, Ck, Cv> VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>
where
    Pk: PrimaryKey,
    Pv: RecordValue,
    Ck: PrimaryKey + From<Pk>,
    Cv: RecordValue + From<Pv>,
{
    pub fn new() -> Self {
        Self {
            _past_key: PhantomData,
            _past_value: PhantomData,
            _current_key: PhantomData,
            _current_value: PhantomData,
        }
    }

    pub fn spawn_with_ctx<P: Actor + Handler<DeadActorResult<Self>>>(
        self,
        ctx: &Ctx<P>,
    ) -> ActorAskRef<UpgradeVersion, UpgradedVersion> {
        let (asker,) = CtxBuilder::new(self).asker::<UpgradeVersion>().spawn(ctx);
        asker
    }
}

/// TODO(Alec): Can this be done in type safe way? Right now, it doesn't look like it
impl<Pk, Pv, Ck, Cv> Ask<UpgradeVersion> for VersionedTreeUpgradeActor<Pk, Pv, Ck, Cv>
where
    Pk: PrimaryKey,
    Pv: RecordValue,
    Ck: PrimaryKey + From<Pk>,
    Cv: RecordValue + From<Pv>,
{
    type Result = UpgradedVersion;

    fn handle(&mut self, upgrade: UpgradeVersion, _: &mut Ctx<Self>) -> Self::Result {
        let past_value: Pv = serde_json::from_slice(&upgrade.past_value).unwrap();
        let past_key: Pk = bincode::deserialize(&upgrade.past_key).unwrap();

        let current_value = Cv::from(past_value);
        let current_key = Ck::from(past_key);

        let current_value_vec = serde_json::to_vec(&current_value).unwrap();
        let current_key_vec = bincode::serialize(&current_key).unwrap();

        UpgradedVersion::new(current_key_vec, current_value_vec)
    }

    // TODO(Alec): Shouldn't this be blocking, there is a lot of serializing and
    // deserializing here.
}

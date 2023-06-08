use crate::actors::tree::PrimaryKey;

macro_rules! auto_increment_id_number_impl {
    ($id: ident, $ty: ty) => {

        #[derive(
            Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
        )]
        pub struct $id($ty);

        impl $id {
            pub fn new(inner: $ty) -> Self {
                Self(inner)
            }
        }

        impl From<$ty> for $id {
            fn from(value: $ty) -> Self {
                $id::new(value)
            }
        }

        impl std::ops::Deref for $id {
            type Target = $ty;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $id {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl Default for $id {
            fn default() -> Self {
                Self(0)
            }
        }

        impl tokactor::Message for $id {}

        impl std::fmt::Display for $id {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl crate::AutoIncrement for $id {
            fn increment(&mut self) -> Self {
                self.0 += 1;
                Self(self.0)
            }
        }

        impl PrimaryKey for $id {}
    };
}

auto_increment_id_number_impl!(U8, u8);
auto_increment_id_number_impl!(U16, u16);
auto_increment_id_number_impl!(U32, u32);
auto_increment_id_number_impl!(U64, u64);
auto_increment_id_number_impl!(U128, u128);

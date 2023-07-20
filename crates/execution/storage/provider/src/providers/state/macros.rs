//! Helper macros for implementing traits for various [StateProvider](crate::StateProvider)
//! implementations

/// A macro that delegates trait implementations to the `as_ref` function of the type.
///
/// Used to implement provider traits.
macro_rules! delegate_impls_to_as_ref {
    (for $target:ty => $($trait:ident $(where [$($generics:tt)*])? {  $(fn $func:ident$(<$($generic_arg:ident: $generic_arg_ty:path),*>)?(&self, $($arg:ident: $argty:ty),*) -> $ret:path;)* })* ) => {

        $(
          impl<'a, $($($generics)*)?> $trait for $target {
              $(
                  fn $func$(<$($generic_arg: $generic_arg_ty),*>)?(&self, $($arg: $argty),*) -> $ret {
                    self.as_ref().$func($($arg),*)
                  }
              )*
          }
        )*
    };
}

pub(crate) use delegate_impls_to_as_ref;

/// Delegates the provider trait implementations to the `as_ref` function of the type:
///
/// [AccountReader](crate::AccountReader)
/// [BlockHashReader](crate::BlockHashReader)
/// [StateProvider](crate::StateProvider)
macro_rules! delegate_provider_impls {
    ($target:ty $(where [$($generics:tt)*])?) => {
        $crate::providers::state::macros::delegate_impls_to_as_ref!(
            for $target =>
            StateRootProvider $(where [$($generics)*])? {
                fn state_root(&self, state: crate::PostState) -> execution_interfaces::Result<execution_primitives::H256>;
            }
            AccountReader $(where [$($generics)*])? {
                fn basic_account(&self, address: execution_primitives::Address) -> execution_interfaces::Result<Option<execution_primitives::Account>>;
            }
            BlockHashReader $(where [$($generics)*])? {
                fn block_hash(&self, number: u64) -> execution_interfaces::Result<Option<execution_primitives::H256>>;
                fn canonical_hashes_range(&self, start: execution_primitives::BlockNumber, end: execution_primitives::BlockNumber) -> execution_interfaces::Result<Vec<execution_primitives::H256>>;
            }
            StateProvider $(where [$($generics)*])?{
                fn storage(&self, account: execution_primitives::Address, storage_key: execution_primitives::StorageKey) -> execution_interfaces::Result<Option<execution_primitives::StorageValue>>;
                fn proof(&self, address: execution_primitives::Address, keys: &[execution_primitives::H256]) -> execution_interfaces::Result<(Vec<execution_primitives::Bytes>, execution_primitives::H256, Vec<Vec<execution_primitives::Bytes>>)>;
                fn bytecode_by_hash(&self, code_hash: execution_primitives::H256) -> execution_interfaces::Result<Option<execution_primitives::Bytecode>>;
            }
        );
    }
}

pub(crate) use delegate_provider_impls;

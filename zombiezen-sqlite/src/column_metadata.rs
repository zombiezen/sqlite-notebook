use std::ffi::{c_char, c_int, CStr, CString};
use std::mem::MaybeUninit;
use std::ptr;

use bitflags::bitflags;
use libsqlite3_sys::sqlite3_table_column_metadata;

use crate::{Conn, Result, ResultCode};

impl Conn {
    /// Return information about the given column of the given table.
    /// `schema` is the name of the database
    /// (i.e. "main", "temp", or an attached database)
    /// If `schema` is `None`, then all attached databases are searched for the table
    /// using the same algorithm used by the database engine
    /// to resolve unqualified table references.
    pub fn table_column_metadata(
        &self,
        schema: Option<&(impl AsRef<CStr> + ?Sized)>,
        table_name: &(impl AsRef<CStr> + ?Sized),
        column_name: &(impl AsRef<CStr> + ?Sized),
        flags: TableColumnMetadataFlags,
    ) -> Result<ColumnMetadata> {
        let mut data_type = MaybeUninit::<*const c_char>::uninit();
        let mut coll_seq = MaybeUninit::<*const c_char>::uninit();
        let mut not_null = MaybeUninit::<c_int>::uninit();
        let mut primary_key = MaybeUninit::<c_int>::uninit();
        let mut autoinc = MaybeUninit::<c_int>::uninit();
        ResultCode(unsafe {
            sqlite3_table_column_metadata(
                self.as_ptr(),
                schema.map_or_else(ptr::null, |s| s.as_ref().as_ptr()),
                table_name.as_ref().as_ptr(),
                column_name.as_ref().as_ptr(),
                if flags.contains(TableColumnMetadataFlags::DATA_TYPE) {
                    data_type.as_mut_ptr()
                } else {
                    ptr::null_mut()
                },
                if flags.contains(TableColumnMetadataFlags::COLLATION_SEQUENCE) {
                    coll_seq.as_mut_ptr()
                } else {
                    ptr::null_mut()
                },
                not_null.as_mut_ptr(),
                primary_key.as_mut_ptr(),
                autoinc.as_mut_ptr(),
            )
        })
        .to_result()?;
        let data_type = if flags.contains(TableColumnMetadataFlags::DATA_TYPE) {
            Some((unsafe { CStr::from_ptr(data_type.assume_init()) }).to_owned())
        } else {
            None
        };
        let coll_seq = if flags.contains(TableColumnMetadataFlags::COLLATION_SEQUENCE) {
            Some((unsafe { CStr::from_ptr(coll_seq.assume_init()) }).to_owned())
        } else {
            None
        };
        let not_null = unsafe { not_null.assume_init() != 0 };
        let primary_key = unsafe { primary_key.assume_init() != 0 };
        let autoinc = unsafe { autoinc.assume_init() != 0 };

        Ok(ColumnMetadata {
            data_type,
            coll_seq,
            not_null,
            primary_key,
            autoinc,
        })
    }
}

/// Information returned by [`Conn::table_column_metadata`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMetadata {
    data_type: Option<CString>,
    coll_seq: Option<CString>,
    not_null: bool,
    primary_key: bool,
    autoinc: bool,
}

impl ColumnMetadata {
    /// Returns the column's declared data type if it was requested.
    #[inline]
    pub fn data_type(&self) -> Option<&CStr> {
        self.data_type.as_ref().map(AsRef::as_ref)
    }

    /// Returns the column's collation sequence name if it was requested.
    #[inline]
    pub fn collation_sequence(&self) -> Option<&CStr> {
        self.coll_seq.as_ref().map(AsRef::as_ref)
    }

    /// Reports whether the column has a `NOT NULL` constraint.
    #[inline]
    pub fn not_null(&self) -> bool {
        self.not_null
    }

    /// Reports whether the column is part of the `PRIMARY KEY`.
    #[inline]
    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    /// Reports whether the column is [`AUTOINCREMENT`].
    ///
    /// [`AUTOINCREMENT`]: https://www.sqlite.org/autoinc.html
    #[inline]
    pub fn is_autoincrement(&self) -> bool {
        self.autoinc
    }
}

bitflags! {
    /// Information to collect for [`Conn::table_column_metadata`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct TableColumnMetadataFlags: u16 {
        const DATA_TYPE = 0x01;
        const COLLATION_SEQUENCE = 0x01;
    }
}

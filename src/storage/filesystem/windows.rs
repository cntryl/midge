//! Handle-based identities for Windows filesystem range operations.

use super::{range_io_error, StorageObjectMetadata};
use std::fs::File;
use std::mem::{size_of, MaybeUninit};
use std::os::windows::io::AsRawHandle;
use winapi::um::fileapi::{FILE_BASIC_INFO, FILE_ID_INFO};
use winapi::um::minwinbase::{FileBasicInfo, FileIdInfo};
use winapi::um::winbase::GetFileInformationByHandleEx;

pub(super) fn range_metadata(file: &File) -> Result<StorageObjectMetadata, String> {
    let metadata = file.metadata().map_err(|error| range_io_error(&error))?;
    if !metadata.is_file() {
        return Err("range reads require an ordinary immutable file".into());
    }
    let mut identity = MaybeUninit::<FILE_ID_INFO>::zeroed();
    let mut basic = MaybeUninit::<FILE_BASIC_INFO>::zeroed();
    // SAFETY: the file owns a live synchronous handle throughout both calls.
    // Each information class matches its writable output structure and size;
    // neither output is read unless both calls report successful initialization.
    let initialized = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle().cast(),
            FileIdInfo,
            identity.as_mut_ptr().cast(),
            u32::try_from(size_of::<FILE_ID_INFO>()).expect("file identity size fits DWORD"),
        ) != 0
            && GetFileInformationByHandleEx(
                file.as_raw_handle().cast(),
                FileBasicInfo,
                basic.as_mut_ptr().cast(),
                u32::try_from(size_of::<FILE_BASIC_INFO>()).expect("file metadata size fits DWORD"),
            ) != 0
    };
    if !initialized {
        return Err(format!(
            "read Windows filesystem range identity: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the successful calls above initialized these exact structures;
    // LARGE_INTEGER exposes each native timestamp through its QuadPart member.
    let (identity, last_write, changed) = unsafe {
        let identity = identity.assume_init();
        let basic = basic.assume_init();
        (
            identity,
            *basic.LastWriteTime.QuadPart(),
            *basic.ChangeTime.QuadPart(),
        )
    };
    Ok(StorageObjectMetadata {
        size: metadata.len(),
        // Keep the fs: discriminator used by conditional deletion. File IDs
        // distinguish replacement; write/change times distinguish in-place
        // mutation. LastAccessTime is excluded because reads may update it.
        etag: format!(
            "fs:win:{:016x}:{:032x}:{last_write}:{changed}",
            identity.VolumeSerialNumber,
            u128::from_le_bytes(identity.FileId.Identifier),
        ),
        generation: None,
    })
}

package io.github.rivomesh.platform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;

/** Atomic controller/config writes; never silently fall back to a non-atomic replacement. */
final class PrivateFiles {
    private PrivateFiles() {}
    static void directory(Path path) throws IOException {
        Files.createDirectories(path);
        if (Files.isSymbolicLink(path)) throw new IOException("SYMLINK_DIRECTORY");
    }
    static void ownerOnly(Path path) throws IOException {
        try { Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------")); }
        catch (UnsupportedOperationException ignored) { /* Platform ACLs must protect the installation directory. */ }
    }
    static byte[] read(Path path,int limit) throws IOException {
        try(java.io.InputStream in=Files.newInputStream(path);java.io.ByteArrayOutputStream out=new java.io.ByteArrayOutputStream()){
            byte[] buffer=new byte[8192];int n;
            while((n=in.read(buffer))!=-1){if(out.size()+n>limit)throw new IOException("CONFIG_TOO_LARGE");out.write(buffer,0,n);}
            return out.toByteArray();
        }
    }
    static void write(Path path, byte[] bytes) throws IOException {
        directory(path.toAbsolutePath().getParent());
        Path temp = Files.createTempFile(path.getParent(), ".rivomesh-", ".tmp");
        try {
            ownerOnly(temp);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel parent = FileChannel.open(path.getParent(), StandardOpenOption.READ)) { parent.force(true); }
            catch (AccessDeniedException | UnsupportedOperationException ignored) { /* Some platforms cannot fsync directories. */ }
        } finally { Files.deleteIfExists(temp); }
    }
}

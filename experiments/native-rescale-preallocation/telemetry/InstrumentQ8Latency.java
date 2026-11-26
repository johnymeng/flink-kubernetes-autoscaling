import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/** Inserts a probe at each of two known instructions without changing the state schema. */
public final class InstrumentQ8Latency {
    private static final String SOURCE = "be/uclouvain/gepiciad/sources/Q8ReplaySource.class";
    private static final String JOIN = "be/uclouvain/gepiciad/nexmark/Query8$1.class";
    private static final String PROBE = "be/uclouvain/gepiciad/nexmark/Q8RecordLatency";
    private static final String SOURCE_CONTEXT =
            "org/apache/flink/streaming/api/functions/source/SourceFunction$SourceContext";
    private static final String COLLECTOR = "org/apache/flink/util/Collector";
    private static final String JOIN_ARGUMENTS =
            "(Lorg/apache/beam/sdk/nexmark/model/Person;Lorg/apache/beam/sdk/nexmark/model/Auction;";

    private static byte[] instrument(byte[] original, boolean source) {
        ClassReader reader = new ClassReader(original);
        ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_MAXS);
        String target = source ? SOURCE_CONTEXT : COLLECTOR;
        String method = source ? "run" : "join";
        String signature = source ? "(L" + SOURCE_CONTEXT + ";)V" : JOIN_ARGUMENTS + "L" + COLLECTOR + ";)V";
        int[] matches = {0};
        reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
            @Override
            public MethodVisitor visitMethod(
                    int access, String name, String descriptor, String generic, String[] exceptions) {
                MethodVisitor visitor = super.visitMethod(access, name, descriptor, generic, exceptions);
                if (!name.equals(method) || !descriptor.equals(signature)) {
                    return visitor;
                }
                return new MethodVisitor(Opcodes.ASM9, visitor) {
                    @Override
                    public void visitMethodInsn(
                            int opcode, String owner, String name, String descriptor, boolean iface) {
                        boolean collect = opcode == Opcodes.INVOKEINTERFACE && owner.equals(target)
                                && name.equals("collect") && descriptor.equals("(Ljava/lang/Object;)V");
                        if (collect && source) {
                            super.visitInsn(Opcodes.DUP);
                            super.visitMethodInsn(Opcodes.INVOKESTATIC, PROBE, "input",
                                    "(Ljava/lang/Object;)V", false);
                        }
                        super.visitMethodInsn(opcode, owner, name, descriptor, iface);
                        if (collect) {
                            matches[0]++;
                            if (!source) {
                                super.visitVarInsn(Opcodes.ALOAD, 1);
                                super.visitVarInsn(Opcodes.ALOAD, 2);
                                super.visitMethodInsn(Opcodes.INVOKESTATIC, PROBE, "output",
                                        JOIN_ARGUMENTS + ")V", false);
                            }
                        }
                    }
                };
            }
        }, 0);
        if (matches[0] != 1) {
            throw new IllegalArgumentException("Expected one " + method + " collection site, got " + matches[0]);
        }
        return writer.toByteArray();
    }

    public static void main(String[] args) throws Exception {
        int changed = 0;
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(Path.of(args[0])));
                ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(Path.of(args[1])))) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                String name = entry.getName();
                if (name.equals(PROBE + ".class") || name.matches("(?i)META-INF/[^/]+\\.(SF|RSA|DSA|EC)")) {
                    throw new IllegalArgumentException("Unexpected signed or already instrumented workload");
                }
                byte[] content = in.readAllBytes();
                if (name.equals(SOURCE) || name.equals(JOIN)) {
                    content = instrument(content, name.equals(SOURCE));
                    changed++;
                }
                out.putNextEntry(new ZipEntry(name));
                out.write(content);
                out.closeEntry();
            }
            if (changed != 2) {
                throw new IllegalArgumentException("Expected to instrument both Q8 classes");
            }
            out.putNextEntry(new ZipEntry(PROBE + ".class"));
            out.write(Files.readAllBytes(Path.of(args[2])));
        }
    }
}

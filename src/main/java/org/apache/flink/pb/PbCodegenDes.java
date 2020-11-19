package org.apache.flink.pb;

/**
 * PbCodegenDes is responsible for converting `messageGetStr` to flink internal row compatible structure by codegen process.
 * The codegen procedure could be considered as `returnVarName` = codegen(`messageGetStr`)
 */
public interface PbCodegenDes {
    /**
     * @param returnVarName the final var name that is calculated by codegen. This var name will be used by outsider codegen environment.
     * @param messageGetStr may be a variable or expression. Current codegen environment can use this literal name directly to access the input.
     * @return
     */
    String codegen(String returnVarName, String messageGetStr);
}

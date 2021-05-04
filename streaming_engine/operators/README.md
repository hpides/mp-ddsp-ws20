# Base Operator Types
## GeneratingOperator
Start of a pipeline. Produces data, when calling execute()

## ConsumingOperator
End of a pipeline. Starts consuming from the pipline by calling the executePipeline() method.

## TransformingOperator
Operator between start and end of a pipeline. Consumes from previous operator and produces for next operator.

# How to create a new Operator:
1. Create a new class _OperatorName (remember to use the underscore).
2. Specify InputType and/or OutputType as template parameters.
2. Inherit from ConsumingOperator, GeneratingOperator or TransformingOperator according to the functionality of the operator. Hand in the template parameters.
3. If you inherit from GeneratingOperator or TransformingOperator, implement the execute() method.
4. If you inherit from ConsumingOperator, implement the executePipeline() method.
3. After the class declaration Write DECLARE_SHARED_PTR(OperatorName) (without underscore) to create the Shared-Pointer Factory-Method.

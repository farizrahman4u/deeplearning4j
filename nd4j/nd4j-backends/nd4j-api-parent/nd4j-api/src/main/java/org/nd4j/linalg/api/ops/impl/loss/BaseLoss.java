/*******************************************************************************
 * Copyright (c) 2015-2019 Skymind, Inc.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License, Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 ******************************************************************************/

package org.nd4j.linalg.api.ops.impl.loss;

import lombok.NonNull;
import org.nd4j.autodiff.loss.LossReduce;
import org.nd4j.autodiff.samediff.SDVariable;
import org.nd4j.autodiff.samediff.SameDiff;
import org.nd4j.base.Preconditions;
import org.nd4j.linalg.api.buffer.DataType;
import org.nd4j.linalg.api.ops.DynamicCustomOp;

import java.util.Collections;
import java.util.List;

public abstract class BaseLoss extends DynamicCustomOp {

    protected LossReduce lossReduce;

    public BaseLoss(@NonNull SameDiff sameDiff, @NonNull LossReduce lossReduce, @NonNull SDVariable predictions, @NonNull SDVariable weights,
                    @NonNull SDVariable labels){
        super(null, sameDiff, new SDVariable[]{predictions, weights, labels});
        this.lossReduce = lossReduce;
        addArgs();
    }

    protected BaseLoss(){ }

    protected void addArgs(){
        iArguments.clear();
        tArguments.clear();
        addIArgument(lossReduce.ordinal()); //Ops: 0 - "none"; 1 - "weighted_sum";  2 - "weighted_mean";  3 - "weighted_sum_by_nonzero_weights"
    }

    public abstract String opName();

    @Override
    public List<DataType> calculateOutputDataTypes(List<DataType> inputDataTypes){
        Preconditions.checkState(inputDataTypes != null && inputDataTypes.size() == 3, "Expected exactly 3 input datatypes for %s, got %s", getClass(), inputDataTypes);
        return Collections.singletonList(inputDataTypes.get(0));    //Same as predictions
    }
}
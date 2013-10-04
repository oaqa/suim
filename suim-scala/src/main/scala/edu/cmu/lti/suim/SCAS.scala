/*
 *  Copyright 2013 Carnegie Mellon University
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.cmu.lti.suim

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataOutput
import java.io.Externalizable
import java.io.ObjectInput
import java.io.ObjectOutput

import org.apache.hadoop.io.Writable
import org.apache.uima.cas.CAS
import org.apache.uima.cas.impl.Serialization
import org.apache.uima.fit.factory.JCasFactory

object SCAS {
       
  def read(in: DataInput) = {
    val scas = new SCAS();
    scas.readFields(in);
    scas
  }
}

class SCAS(val cas: CAS) extends Externalizable with Writable {

  def this() {
    this(JCasFactory.createJCas().getCas())
  }

  override def readExternal(in: ObjectInput) {
    readFields(in)
  }

  override def writeExternal(out: ObjectOutput) {
    write(out)
  }

  def jcas = cas.getJCas()

  override def write(out: DataOutput) {
    val baos = new ByteArrayOutputStream();
    Serialization.serializeWithCompression(cas, baos)
    out.writeInt(baos.size)
    out.write(baos.toByteArray)
  }
       
  override def readFields(in: DataInput) {
    val size = in.readInt();
    val bytes = new Array[Byte](size)
    in.readFully(bytes);
    val bais = new ByteArrayInputStream(bytes)
    Serialization.deserializeCAS(cas, bais);
  }
}

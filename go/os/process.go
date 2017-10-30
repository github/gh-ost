/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package os

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"io/ioutil"
	"os"
	"os/exec"
)

func execCmd(commandText string, arguments ...string) (*exec.Cmd, string, error) {
	// 将CommandText写入一个文本文件，然后再通过bash执行
	commandBytes := []byte(commandText)
	tmpFile, err := ioutil.TempFile("", "gh-ost-process-cmd-")
	if err != nil {
		log.ErrorErrorf(err, "execCmd failed")
		return nil, "", err
	}
	ioutil.WriteFile(tmpFile.Name(), commandBytes, 0644)
	log.Debugf("execCmd: %s", commandText)
	shellArguments := append([]string{}, tmpFile.Name())
	shellArguments = append(shellArguments, arguments...)
	log.Debugf("%+v", shellArguments)
	return exec.Command("bash", shellArguments...), tmpFile.Name(), nil
}

// CommandRun executes a command
func CommandRun(commandText string, arguments ...string) error {
	cmd, tmpFileName, err := execCmd(commandText, arguments...)
	defer os.Remove(tmpFileName)
	if err != nil {
		log.ErrorErrorf(err, "CommandRun failed")
		return err
	}
	err = cmd.Run()
	log.ErrorErrorf(err, "CommandRun failed")
	return err
}

// RunCommandWithOutput executes a command and return output bytes
func RunCommandWithOutput(commandText string) ([]byte, error) {
	cmd, tmpFileName, err := execCmd(commandText)
	defer os.Remove(tmpFileName)
	if err != nil {
		log.ErrorErrorf(err, "RunCommandWithOutput failed")
		return nil, err
	}

	outputBytes, err := cmd.Output()
	if err != nil {
		log.ErrorErrorf(err, "RunCommandWithOutput failed")
		return nil, err
	}

	return outputBytes, nil
}

// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.ql.ast.util;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intercept syntactical errors and log them.
 * The default ANTLR4 behavior is to write error information out to stderr. To
 * prevent this, we must remove the default error listeners and add an instance
 * of this class instead.
 * @since 2.3
 */
public class LoggingErrorListener extends BaseErrorListener {
  /** The logger to write errors will be written. */
  private final Logger logger;

  public LoggingErrorListener() {
    logger = LoggerFactory.getLogger(LoggingErrorListener.class);
  }

  @Override
  public void syntaxError(
    final Recognizer<?,?> recognizer,
    final Object offending_symbol,
    final int line,
    final int char_position_in_line,
    final String msg,
    final RecognitionException e) {
    final String report =
        String.format("%d:%d: %s", line, char_position_in_line, msg);
    logger.error(report);
  }
}


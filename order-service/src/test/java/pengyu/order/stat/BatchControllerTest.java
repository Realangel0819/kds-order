package pengyu.order.stat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 컨트롤러가 JobParameters를 제대로 만들어 JobLauncher에 넘기는지만 본다. (Job 자체는 DailyCookStatJobTest에서 검증)
 */
@WebMvcTest(BatchController.class)
class BatchControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    JobLauncher jobLauncher;

    @MockBean(name = "dailyCookStatJob")
    Job dailyCookStatJob;

    @Test
    @DisplayName("targetDate와 실행 시각(time)을 JobParameters로 넘겨 배치를 실행한다")
    void launchesJobWithParameters() throws Exception {
        JobExecution execution = new JobExecution(7L);
        execution.setStatus(BatchStatus.COMPLETED);
        when(jobLauncher.run(eq(dailyCookStatJob), any())).thenReturn(execution);

        mockMvc.perform(post("/api/batch/cook-stat").param("targetDate", "2026-10-05"))
                .andExpect(status().isOk())
                .andExpect(content().string("2026-10-05 일일 조리 통계 배치 실행 결과: COMPLETED (jobExecutionId=7)"));

        ArgumentCaptor<JobParameters> captor = ArgumentCaptor.forClass(JobParameters.class);
        verify(jobLauncher).run(eq(dailyCookStatJob), captor.capture());
        assertThat(captor.getValue().getString("targetDate")).isEqualTo("2026-10-05");
        assertThat(captor.getValue().getLong("time")).isNotNull();
    }

    @Test
    @DisplayName("날짜 형식이 틀리면 배치를 실행하지 않고 400을 돌려준다")
    void rejectsInvalidDate() throws Exception {
        mockMvc.perform(post("/api/batch/cook-stat").param("targetDate", "2026-13-01"))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(jobLauncher);
    }
}
